import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.persistence.{PersistenceSettings, PersistentRepr}
import akka.persistence.hbase.journal.{HBaseClientFactory, PluginPersistenceSettings}
import akka.persistence.hbase.common.Const._
import akka.persistence.hbase.common.Columns._
import akka.persistence.hbase.common._
import akka.persistence.serialization.Snapshot
import com.typesafe.config._
import com.coinport.bitway.serializers.ThriftJsonSerializer
import java.util.{ArrayList => JArrayList}
import java.io.{Closeable, OutputStreamWriter, BufferedWriter, BufferedInputStream}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.collection.JavaConverters._

import DeferredConversions._

object EventSourceUtil extends App {

  override def main(args: Array[String]) {
    if (args.length < 1) {
      sys.error("Enter tool[dumpMessages, dumpSnapshot, verifyRank] to run and processorId exceeded for dumpEvent")
      sys.exit(0)
    }

    if ("dumpMessages".equals(args(0)) && args.length >= 2) {
      dumpMessages(args)
    } else if ("dumpSnapshot".equals(args(0)) && args.length >= 2) {
      dumpSnapshot(args)
    } else {
      sys.error("Enter tool[dumpMessages, dumpSnapshot, verifyRank] to run and processorId exceeded for dumpEvent")
    }

  }

  def dumpMessages(args: Array[String]) {
    val processorId: String = args(1)
    implicit val configFile: String = "dump.conf"
    val messagesDumper = new DoDumpEventSource(exportMessages)
    val startSeq = (args.length >= 3) match {
      case true => java.lang.Long.parseLong(args(2))
      case false => 1L
    }
    val stopSeq = (args.length >= 4) match {
      case true => java.lang.Long.parseLong(args(3))
      case false => Long.MaxValue
    }
    messagesDumper.exportData(processorId, startSeq, stopSeq)
  }

  def dumpSnapshot(args: Array[String]) {
    val processorId: String = args(1)
    implicit val configFile: String = "dump.conf"
    val snapshotDumper = new DumpSnapshot()
    snapshotDumper.dumpSnapshot(processorId)
  }

  def exportMessages(messages: List[(Long, Any)], lastSeqNum: Long, processorId: String, exportMessagesHdfsDir: String, fs: FileSystem, isEnd: Boolean) {
    if (messages.isEmpty || isEnd)
      return
    val builder = new StringBuilder()
    val converter = new ThriftJsonSerializer()
    for ((seqNum, msg) <- messages) {
      builder ++= s"""{"${msg.getClass.getEnclosingClass.getSimpleName}":${new String(converter.toBinary(msg.asInstanceOf[AnyRef]), "UTF-8")},"${Bytes.toString(SequenceNr)}":${seqNum.toString}}\n"""
    }
    writeMessages(builder.toString, lastSeqNum, processorId)

    def writeMessages(data: String, seqNum: Long, processorId: String) {
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(
        new Path(exportMessagesHdfsDir, s"coinport_events_${processorId}_${String.valueOf(seqNum).reverse.padTo(16, "0").reverse.mkString}_v1.json".toLowerCase))))
      writer.write( s"""{"timestamp": ${System.currentTimeMillis()},\n"events": [""")
      writer.write(data.substring(0, data.length - 1))
      writer.write("]}")
      writer.flush()
      writer.close()
    }

  }
}

object writeVerifyRank {
  val verifiedUser = collection.mutable.Set.empty[Long]
  var lastRank: Int = 1
  var verifyWriter: BufferedWriter = null
  var builder = new StringBuilder()

  def writeRank(data: String, exportVerifyRankHdfsDir: String, fs: FileSystem, isEnd: Boolean) {
    if (verifyWriter == null) {
      val format = new java.text.SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
      verifyWriter = new BufferedWriter(new OutputStreamWriter(fs.create(
        new Path(exportVerifyRankHdfsDir, s"coinport_phone_verified_users_${format.format(new java.util.Date(System.currentTimeMillis()))}.csv".toLowerCase))))
    }
    if (!data.isEmpty) {
      verifyWriter.write(data)
      verifyWriter.flush()
    }
    if (isEnd) {
      verifyWriter.close()
    }
  }

}


trait DumpEventCommon {
  implicit val configFile: String
  protected val config = ConfigFactory.load(getClass.getClassLoader, configFile)
  protected val fs: FileSystem = openHdfsSystem(config.getString("dump.hdfsHost"))
  protected implicit val system = ActorSystem("test", config)
  protected val cryptKey = config.getString("dump.encryption-settings")
  implicit var pluginPersistenceSettings = PluginPersistenceSettings(config, JOURNAL_CONFIG)
  implicit var executionContext = system.dispatcher
  implicit var serialization = EncryptingSerializationExtension(system, cryptKey)
  implicit val logger: LoggingAdapter = system.log
  protected val BUFFER_SIZE = 2048

  private def openHdfsSystem(defaultName: String): FileSystem = {
    val conf = new Configuration()
    conf.set("fs.default.name", defaultName)
    FileSystem.get(conf)
  }
}

class DoDumpEventSource(func: (List[(Long, Any)], Long, String, String, FileSystem, Boolean) => Unit)(implicit val configFile: String) extends DumpEventCommon {
  private val messagesTable = config.getString("dump.table")
  private val messagesFamily = config.getString("dump.family")
  private val exportMessagesHdfsDir = config.getString("dump.dumpMessagesHdfsDir")
  private val SCAN_MAX_NUM_ROWS = 5
  private val ReplayGapRetry = 5

  val StartSeqNum: Int = 1
  val client = getHBaseClient()

  def exportData(processorId: String, fromSeqNum: Long = StartSeqNum, toSeqNum: Long = Long.MaxValue) = {
    dumpMessages(processorId, fromSeqNum, toSeqNum)
  }

  // "fromSeqNum" is inclusive, "toSeqNum" is exclusive
  def dumpMessages(processorId: String, fromSeqNum: Long, toSeqNum: Long): Future[Unit] = {
    if (toSeqNum <= fromSeqNum) return Future(())
    var retryTimes: Int = 0
    var isDuplicate = false
    var tryStartSeqNr: Long = if (fromSeqNum <= 0) 1 else fromSeqNum

    var scanner: SaltedScanner = null
    type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

    def hasSequenceGap(columns: collection.mutable.Buffer[KeyValue]): Boolean = {
      val processingSeqNr = sequenceNr(columns)
      if (tryStartSeqNr != processingSeqNr) {
        if (tryStartSeqNr > processingSeqNr) {
          sys.error(s"Replay $processorId Meet duplicated message: to process is $tryStartSeqNr, actual is $processingSeqNr")
          isDuplicate = true
        }
        return true
      } else {
        return false
      }
    }

    def initScanner() {
      if (scanner != null) scanner.close()
      scanner = new SaltedScanner(client, pluginPersistenceSettings.partitionCount, Bytes.toBytes(messagesTable), Bytes.toBytes(messagesFamily))
      scanner.setSaltedStartKeys(processorId, tryStartSeqNr)
      scanner.setSaltedStopKeys(processorId, RowKey.toSequenceNr(toSeqNum))
      scanner.setKeyRegexp(processorId)
      scanner.setMaxNumRows(SCAN_MAX_NUM_ROWS)
    }

    def sequenceNr(columns: mutable.Buffer[KeyValue]): Long = {
      for (column <- columns) {
        if (java.util.Arrays.equals(column.qualifier, SequenceNr)) {
          return Bytes.toLong(column.value())
        }
      }
      0L
    }

    def getMessages(rows: AsyncBaseRows): (Boolean, String, List[(Long, Any)]) = {
      val messages = ListBuffer.empty[(Long, Any)]
      for (row <- rows.asScala) {
        if (hasSequenceGap(row.asScala) && retryTimes < ReplayGapRetry) {
          if (isDuplicate) {
            return (true, "Duplicated message", List.empty[(Long, Any)])
          }
          sys.error(s"Meet gap at ${tryStartSeqNr}")
          retryTimes += 1
          Thread.sleep(100)
          initScanner()
          return (false, "", List.empty[(Long, Any)])
        } else {
          if (retryTimes >= ReplayGapRetry) {
            return (true, s"Gap retry times reach ${ReplayGapRetry}", List.empty[(Long, Any)])
          }
          var seqNum = 0L
          var payload: Any = null
          for (column <- row.asScala) {
            if (java.util.Arrays.equals(column.qualifier, Message) || java.util.Arrays.equals(column.qualifier, SequenceNr)) {
              if (java.util.Arrays.equals(column.qualifier, Message)) {
                // will throw an exception if failed
                payload = serialization.deserialize(column.value(), classOf[PersistentRepr]).payload
              } else {
                seqNum = Bytes.toLong(column.value())
                tryStartSeqNr = seqNum + 1
              }
            }
          }
          messages.append((seqNum, payload))
          retryTimes = 0
        }
      }
      (false, "", messages.toList)
    }

    def handleRows(): Future[Unit] = {
      scanner.nextRows() map {
        case null =>
          scanner.close()
          func(List.empty, tryStartSeqNr - 1, processorId, exportMessagesHdfsDir, fs, true)
          client.shutdown()
          fs.close()
          sys.exit(0)
          Future(())
        case rows: AsyncBaseRows =>
          val (isFailed, errMsg, messages) = getMessages(rows)
          if (tryStartSeqNr > 0 && !messages.isEmpty) {
            func(messages, tryStartSeqNr - 1, processorId, exportMessagesHdfsDir, fs, false)
          }
          if (isFailed) {
            sys.error(errMsg)
            Future.failed(new Exception(errMsg))
          } else {
            handleRows()
          }
      }
    }

    initScanner
    handleRows()
  }

  private def getHBaseClient() = {
    val hBasePersistenceSettings = PluginPersistenceSettings(config, "hbase-journal")
    HBaseClientFactory.getClient(hBasePersistenceSettings, new PersistenceSettings(config.getConfig("akka.persistence")))
  }

}

class DumpSnapshot(implicit val configFile: String) extends DumpEventCommon {
  private val exportSnapshotHdfsDir = config.getString("dump.dumpSnapshotHdfsDir")
  private val snapshotHdfsDir: String = config.getString("dump.snapshot-dir")

  def dumpSnapshot(processorId: String, processedSeqNum: Long = 1): Long = {
    val snapshotMetas = listSnapshots(snapshotHdfsDir, processorId)
    if (snapshotMetas.isEmpty) //no file to process, let processedSeqNum to former process's lastNum, which is processedSeqNum - 1
      return processedSeqNum - 1
    snapshotMetas.head match {
      // when lastSeqNum == processedSeqNum, there is one message
      case desc@HdfsSnapshotDescriptor(processorId: String, seqNum: Long, _) if (seqNum >= processedSeqNum) =>
        val path = new Path(snapshotHdfsDir, desc.toFilename)
        val snapshot =
          serialization.deserialize(
            withStream(new BufferedInputStream(fs.open(path, BUFFER_SIZE), BUFFER_SIZE)) {
              IOUtils.toByteArray
            }, classOf[Snapshot])
        val className = snapshot.data.getClass.getEnclosingClass.getSimpleName
        writeSnapshot(exportSnapshotHdfsDir, processorId, seqNum, snapshot, className)
        seqNum
      case _ => processedSeqNum - 1
    }
  }

  def writeSnapshot(outputDir: String, processorId: String, seqNum: Long, snapshot: Snapshot, className: String) {
    val converter = new ThriftJsonSerializer()
    val json = new String(converter.toBinary(snapshot), "UTF-8")
    val jsonSnapshot = s"""{"timestamp": ${System.currentTimeMillis()},\n"${className}": ${json}}"""
    val exportSnapshotPath = new Path(outputDir,
      s"coinport_snapshot_${processorId}_${String.valueOf(seqNum).reverse.padTo(16, "0").reverse.mkString}_v1.json".toLowerCase)
    withStream(new BufferedWriter(new OutputStreamWriter(fs.create(exportSnapshotPath, true)), BUFFER_SIZE))(IOUtils.write(jsonSnapshot, _))
    fs.close()
    sys.exit(0)
  }

  private def listSnapshots(snapshotDir: String, processorId: String): Seq[HdfsSnapshotDescriptor] = {
    val descs = fs.listStatus(new Path(snapshotDir)) flatMap {
      HdfsSnapshotDescriptor.from(_, processorId)
    }
    if (descs.isEmpty) Nil else descs.sortWith(_.seqNumber > _.seqNumber).toSeq
  }

  private def withStream[S <: Closeable, A](stream: S)(fun: S => A): A =
    try fun(stream) finally stream.close()

}
