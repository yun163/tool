

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoConnection, MongoURI}
import com.coinport.bitway.data._
import com.coinport.bitway.serializers.ThriftEnumJson4sSerialization
import com.mongodb.util.JSON
import org.json4s.CustomSerializer
import org.json4s._
import org.json4s.ext._

sealed trait SimpleMongoCollection[T <: AnyRef] {
  val coll: MongoCollection
  val DATA = "data"
  val ID = "_id"

  def extractId(obj: T): Long

  def get(id: Long): Option[T]

  def put(data: T): Unit
}

class FeeSerializer(implicit man: Manifest[Fee.Immutable]) extends CustomSerializer[Fee](format => ( {
  case obj: JValue =>
    val data = Extraction.extract(obj)(ThriftEnumJson4sSerialization.formats, man)
    println(s"Run to extract: obj -> ${obj.toString},  data -> ${data}")
    data
  //      new Fee.I mmutable(obj.children)
  //fee
}, {
  case x: Fee =>
    val data = Extraction.decompose(x)(ThriftEnumJson4sSerialization.formats)
    println(s"Run to decompose: x -> ${x.toString},  data -> ${data}")
    data
  //      val obj = new JObject(List.empty[JField])
  //      obj
}))

class BankAccountSerializer(implicit man: Manifest[BankAccount.Immutable]) extends CustomSerializer[BankAccount](format => ({
  case obj: JValue => Extraction.extract(obj)(ThriftEnumJson4sSerialization.formats, man)
}, {
  case x: BankAccount => Extraction.decompose(x)(ThriftEnumJson4sSerialization.formats)
}))

class SellCoinDataSerializer(implicit man: Manifest[SellCoinData.Immutable]) extends CustomSerializer[SellCoinData](format => ({
  case obj: JValue => Extraction.extract(obj)(ThriftEnumJson4sSerialization.formats + new BankAccountSerializer(), man)
}, {
  case x: SellCoinData => Extraction.decompose(x)(ThriftEnumJson4sSerialization.formats + new BankAccountSerializer())
}))

abstract class SimpleJsonMongoCollection[T <: AnyRef, S <: T](implicit man: Manifest[S]) extends SimpleMongoCollection[T] {
  import org.json4s.native.Serialization.{read, write}
  implicit val formats = ThriftEnumJson4sSerialization.formats + new FeeSerializer

  def get(id: Long) = coll.findOne(MongoDBObject(ID -> id)) map {
    json => read[S](json.get(DATA).toString)
  }

  def put(data: T) = coll += MongoDBObject(ID -> extractId(data), DATA -> JSON.parse(write(data)))

  def find(q: MongoDBObject, skip: Int, limit: Int): Seq[T] =
    coll.find(q).sort(MongoDBObject(ID -> -1)).skip(skip).limit(limit).map {
      json => read[S](json.get(DATA).toString)
    }.toSeq

  def count(q: MongoDBObject): Long = coll.count(q)
}

object TestSerializer extends App {
  override def main(args: Array[String]) = {
    val transferHandler = new SimpleJsonMongoCollection[AccountTransfer, AccountTransfer.Immutable]() {
      val mongoUriForViews = MongoURI("mongodb://localhost:27017/test")
      val mongoForViews = MongoConnection(mongoUriForViews)
      val db = mongoForViews(mongoUriForViews.database.get)
      lazy val coll = db("testColl")

      def extractId(item: AccountTransfer) = item.id

      def getQueryDBObject(q: QueryTransfer): MongoDBObject = {
        var query = MongoDBObject()
        if (q.currency.isDefined) query ++= MongoDBObject(DATA + "." + AccountTransfer.CurrencyField.name -> q.currency.get.name)
        if (q.types.nonEmpty) query ++= $or(q.types.map(t => DATA + "." + AccountTransfer.TypeField.name -> t.toString): _*)
        if (q.status.isDefined) query ++= MongoDBObject(DATA + "." + AccountTransfer.StatusField.name -> q.status.get.name)
        if (q.spanCur.isDefined) query ++= (DATA + "." + AccountTransfer.CreatedField.name $lte q.spanCur.get.from $gte q.spanCur.get.to)
        query
      }
    }
    val tsf = AccountTransfer(10000L, 2000L, TransferType.Deposit, Currency.Btc, 1000, fee = Some(Fee(10001L, None, Currency.Btc, amount = 20L)))
    transferHandler.put(tsf)
    val data = transferHandler.get(10000L)
    println("@" * 50 + data.toString)
  }
}