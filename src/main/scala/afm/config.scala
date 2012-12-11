package afm

import com.typesafe.config._
import scala.collection._
import scala.collection.immutable.Map
//import com.mongodb.casbah.Imports._
import com.typesafe.config.ConfigValue

import afm.model._
//import afm.mongo._
import afm.scanner._
import afm.distance._
import afm.duplicates._
import afm.dnet._
import afm.io._

object OptionalConfigFactory {
  def load(fileName: String) = new OptionalConfig(ConfigFactory.parseFile(new java.io.File(fileName)))
}

class OptionalConfig(val conf: com.typesafe.config.Config) {

  def safe[A](getter: String => A)(implicit path: String) = if (conf.hasPath(path)) Some(getter(path)) else None

  def getInt(implicit path: String) = safe(conf.getInt)
  def getString(implicit path: String) = safe(conf.getString)
  def getDouble(implicit path: String) = safe(conf.getDouble)
  def getObject(implicit path: String) = safe(conf.getObject)
  def getBoolean(implicit path: String) = safe(conf.getBoolean)
}

/////

trait ConfigProvider {
  implicit val config: Config
}

/*! Configuration
 */
trait Config {
  val fields: List[FieldDef[_]]
  val strictFields: List[FieldDef[_]]

  val identifierFieldDef: FieldDef[_]

  /*! Some of the object we construct here might need this configuration instance */
  implicit val me: Config = this

  //  private val mongoDb = MongoConnection()("driver_small")
  //  val source = new MongoDBSource(mongoDb("md"), new DNetMongoDBAdapter)

  //  lazy val mongoDb = MongoConnection(mongoHostname)(mongoDbName)
  //  lazy val source = new MongoDBSource(mongoDb(mongoSourceCollection), mongoAdapter)
  val source: Source

  //  lazy val source = new DBSource()
  //def collector: Collector = new GroupingMongoDBCollector(mongoDb(mongoCandidatesCollection))
  def collector: Collector

  /*
  def mongoAdapter: Adapter[DBObject] = new BSONAdapter

  def mongoHostname = "localhost"
  def mongoDbName = "pace"
  def mongoSourceCollection: String = "people"
  def mongoCandidatesCollection = "candidate"
*/

  def cores: Option[Int] = None
  def limit: Option[Int] = None
  def windowSize = 10
  def threshold = 0.90

  def sortOn = identifierField
  def compareOn = sortOn
  def identifierField = "n"

  def ngramSize = 3
  def maxNgrams = 4

  def simhashRotationStep = 2
  def simhashAlgo: Simhash = new AdditiveSimhash()

  //def scanner: Scanner = new SingleFieldScanner
  def scanner: Scanner
  def duplicateDetector: Duplicates = new SortedNeighborhood

  def blockingPrefix = 4

  def progressStep = 1000
  def threadPoolBoost = 4
}

trait OverrideConfig extends Config {
  val conf = OptionalConfigFactory.load("conf/pace.conf")

  /*
  override def mongoHostname = conf.getString("pace.mongo.hostName").getOrElse(super.mongoHostname)
  override def mongoDbName = conf.getString("pace.mongo.dbName").getOrElse(super.mongoDbName)
  override def mongoSourceCollection = conf.getString("pace.mongo.sourceCollection").get // OrElse(super.mongoSourceCollection)
  override def mongoCandidatesCollection = conf.getString("pace.mongo.candidatesCollection").getOrElse(super.mongoCandidatesCollection)
  override def mongoAdapter = conf.getString("pace.mongo.adapter") match {
    case Some("attributes") => new BSONAdapter
    case Some("dnet") => new DNetMongoDBAdapter
    case None => super.mongoAdapter
  }
*/

  override def cores = conf.getInt("pace.cores")
  override def limit = conf.getInt("pace.limit")
  override def windowSize = conf.getInt("pace.windowSize").getOrElse(super.windowSize)
  override def threshold = conf.getDouble("pace.threshold").getOrElse(super.threshold)

  override def sortOn = conf.getString("pace.sortOn").getOrElse(super.sortOn)
  override def compareOn = conf.getString("pace.compareOn").getOrElse(super.compareOn)
  override def identifierField = conf.getString("pace.identifierField").getOrElse(super.identifierField)

  override def ngramSize = conf.getInt("pace.ngramSize").getOrElse(super.ngramSize)
  override def maxNgrams = conf.getInt("pace.maxNgrams").getOrElse(super.maxNgrams)

  override def simhashRotationStep = conf.getInt("pace.simhashRotationStep").getOrElse(super.simhashRotationStep)
  override def simhashAlgo = conf.getString("pace.simhash.algo") match {
    case Some("additive") => new AdditiveSimhash()
    case Some("balanced") => new BalancedSimhash()
    case Some("max") => new MaxSimhash(conf.getInt("pace.max.repeat").getOrElse(4))
    case None => super.simhashAlgo
  }

  /**
   * override def scanner = conf.getString("pace.algo") match {
   *
   * case Some("singleField") => new SingleFieldScanner
   * case Some("mergedSimhash") => new MergedSimhashScanner
   * case Some("simhash") => new MultiPassSimhashScanner
   * case Some("ngram") => new NgramScanner
   * case None => super.scanner
   * }
   */

  override def duplicateDetector = conf.getString("pace.detector") match {
    case Some("sortedNeighborhood") => new SortedNeighborhood
    case Some("blocking") => new Blocking
    case None => super.duplicateDetector
  }

  override def blockingPrefix = conf.getInt("pace.blocking.prefix").getOrElse(super.blockingPrefix)

  override def progressStep = conf.getInt("pace.progress.step").getOrElse(super.progressStep)
  override def threadPoolBoost = conf.getInt("pace.threadPoolBoost").getOrElse(super.threadPoolBoost)
}

trait ConfigurableModel extends OverrideConfig {
  override val fields = parseFields()
  override val strictFields = parseFields(".strict")

  def parseFields(base: String = ""): List[FieldDef[_]] = {
    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._

    conf.getObject("pace.model") match {
      case Some(model) => {
        def parseField(name: String) = {
          val weight = conf.getDouble("pace.model%s.%s.weight".format(base, name)).getOrElse(1.0)
          val ignoreMissing = conf.getBoolean("pace.model%s.%s.ignoreMissing".format(base, name)).getOrElse(false)

          val algo = conf.getString("pace.model%s.%s.algo".format(base, name)) match {
            case Some("JaroWinkler") => JaroWinkler(_)
            case Some("Levenstein") => Levenstein(_)
            case Some("Level2JaroWinkler") => Level2JaroWinkler(_)
            case Some("Level2Levenstein") => Level2Levenstein(_)
            case Some("Null") => (_: Double) => NullDistanceAlgo()
            case None => (_: Double) => NullDistanceAlgo()
          }

          val field = conf.getString("pace.model%s.%s.type".format(base, name)) match {
            case Some("Int") => IntFieldDef(_, _, _)
            case Some("String") => StringFieldDef(_, _, _)
            case Some("List") => ListFieldDef(_, _, _)
            case None => StringFieldDef(_, _, _)
          }

          field(name, algo(weight), ignoreMissing)
        }

        model.keySet.filter(k => k != "strict").map(parseField).toList
      }
      case None => List()
    }
  }

  override val identifierFieldDef = new StringFieldDef(identifierField, NullDistanceAlgo(), false)
}
