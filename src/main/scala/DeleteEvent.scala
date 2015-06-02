import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.persistence.hbase.common.Columns._
import akka.persistence.hbase.common.Const._
import akka.persistence.hbase.common.{RowKey, SaltedScanner, EncryptingSerializationExtension}
import akka.persistence.hbase.journal.{HBaseClientFactory, PluginPersistenceSettings}
import com.typesafe.config.ConfigFactory
import com.coinport.bitway.serializers.ThriftJsonSerializer
import com.coinport.bitway.data._
import akka.persistence.{PersistenceSettings, PersistentRepr}
import java.io.{OutputStreamWriter, BufferedWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{DeleteRequest, KeyValue}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.collection.JavaConverters._
import DeferredConversions._

object DeleteEvent extends App {

  override def main(args: Array[String]) {
    if (args.length < 1) {
      sys.error("Enter config to delete")
      sys.exit(0)
    }
    new DoDeleteEvent(args(0), exportMessages).doDelete()
  }

  def exportMessages(messages: List[String], lastSeqNum: Long, processorId: String, exportMessagesHdfsDir: String, fs: FileSystem, isEnd: Boolean) {
    if (messages.isEmpty || isEnd)
      return
    writeMessages(messages.mkString("\n"), lastSeqNum, processorId)

    def writeMessages(data: String, seqNum: Long, processorId: String) {
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(
        new Path(exportMessagesHdfsDir, s"coinport_events_${processorId}_${String.valueOf(seqNum).reverse.padTo(16, "0").reverse.mkString}_v1.json".toLowerCase))))
      writer.write(s"$data")
      writer.flush()
      writer.close()
    }

  }
}

class DoDeleteEvent(toDeleteConfig: String, func: (List[String], Long, String, String, FileSystem, Boolean) => Unit) {
  val configFile: String = "delete.conf"
  protected val config = ConfigFactory.load(getClass.getClassLoader, configFile)
  protected implicit val system = ActorSystem("test", config)
  protected val fs: FileSystem = openHdfsSystem(config.getString("delete.hdfsHost"))
  protected val cryptKey = config.getString("delete.encryption-settings")
  private val messagesTable = config.getString("delete.table")
  private val messagesFamily = config.getString("delete.family")

  private val exportMessagesHdfsDir = config.getString("delete.dumpMessagesHdfsDir")
  implicit var pluginPersistenceSettings = PluginPersistenceSettings(config, JOURNAL_CONFIG)
  implicit var executionContext = system.dispatcher
  implicit var serialization = EncryptingSerializationExtension(system, cryptKey)
  implicit val logger: LoggingAdapter = system.log
  protected val BUFFER_SIZE = 2048
  private val SCAN_MAX_NUM_ROWS = 5
  val client = getHBaseClient()

  private def openHdfsSystem(defaultName: String): FileSystem = {
    val conf = new Configuration()
    conf.set("fs.default.name", defaultName)
    FileSystem.get(conf)
  }

  import java.util.{ArrayList => JArrayList}

  def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr])

  def doDelete() {
    java.lang.Thread.sleep(500)
    val processors = scala.io.Source.fromFile(toDeleteConfig).getLines.filterNot(_.startsWith("#"))
      if (processors.isEmpty) {
        HBaseClientFactory.shutDown()
        fs.close()
      } else {
        processors.foreach(innerDelete)
      }
  }

  def innerDelete(processor: String) {
    val id2Seq = processor.trim.split(",")
    if (id2Seq.size != 3) {
      logger.error(s"invalid config $processor")
    }
    val processorId = id2Seq(0).trim
    val beginSeqNr: Long = java.lang.Long.parseLong(id2Seq(1).trim)
    val toSeqNr = (java.lang.Long.parseLong(id2Seq(2).trim) * 0.9).toLong
    if (processor.isEmpty) {
      logger.error(s"invalid config $processor")
      return
    }
    deleteMessages(processorId, beginSeqNr, toSeqNr)
  }

  def deleteMessages(processorId: String, fromSeqNum: Long, toSeqNum: Long): Future[Unit] = {
    if (toSeqNum <= fromSeqNum) return Future(())
    var tryStartSeqNr: Long = if (fromSeqNum <= 0) 1 else fromSeqNum

    var scanner: SaltedScanner = null
    type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

    def nextSeqNr(columns: collection.mutable.Buffer[KeyValue]): Option[Long] = {
      val processingSeqNr = sequenceNr(columns)
      if (processingSeqNr > 0L) {
        return Some(processingSeqNr)
      } else {
        None
      }
    }

    def initScanner() {
      if (scanner != null) scanner.close()
      scanner = new SaltedScanner(serialization, client, pluginPersistenceSettings.partitionCount, Bytes.toBytes(messagesTable), Bytes.toBytes(messagesFamily))
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

    def getMessages(rows: AsyncBaseRows): List[String] = {
      val messages = ListBuffer.empty[String]
      for (row <- rows.asScala) {
        tryStartSeqNr = nextSeqNr(row.asScala) match {
          case Some(sq) =>
            (tryStartSeqNr to sq) foreach {
              i =>
                removeEvent(processorId, i)
                messages.append(String.valueOf(i))
            }
            sq +1
          case None =>
            removeEvent(processorId, tryStartSeqNr)
            messages.append(String.valueOf(tryStartSeqNr))
            tryStartSeqNr + 1
        }
      }
      messages.toList
    }

    def removeEvent(id: String, seqNr: Long) {
      val request = new DeleteRequest(Bytes.toBytes(messagesTable), RowKey(processorId, seqNr).toBytes)
      println(s">>>>>>>>>>>>>>>>>>>> delete $id at $seqNr")
      client.delete(request)
    }

    def handleRows(): Future[Unit] = {
      scanner.nextRows() map {
        case null =>
          scanner.close()
          func(List.empty, tryStartSeqNr - 1, processorId, exportMessagesHdfsDir, fs, true)
          client.shutdown()
          sys.exit(0)
          Future(())
        case rows: AsyncBaseRows =>
          val messages = getMessages(rows)
          if (tryStartSeqNr > 0 && !messages.isEmpty) {
            func(messages, tryStartSeqNr - 1, processorId, exportMessagesHdfsDir, fs, false)
          }
          handleRows()
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