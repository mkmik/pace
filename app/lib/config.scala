package afm

import com.typesafe.config._
import scala.collection._
import scala.collection.immutable.Map
import com.mongodb.casbah.Imports._
import com.typesafe.config.ConfigValue

import afm.model._
import afm.mongo._
import afm.scanner._
import afm.distance._
import afm.duplicates._


object OptionalConfigFactory {
  def load(fileName: String) = new OptionalConfig(ConfigFactory.load(fileName))
}


class OptionalConfig(val conf: com.typesafe.config.Config) {

  def safe[A](getter: String => A)(implicit path: String) = if (conf.hasPath(path)) Some(getter(path)) else None

  def getInt(implicit path: String) = safe(conf.getInt)
  def getString(implicit path: String) = safe(conf.getString)
  def getDouble(implicit path: String) = safe(conf.getDouble)
  def getObject(implicit path: String) = safe(conf.getObject)
}


/////

trait ConfigProvider {
  implicit val config: Config
} 

/*! Configuration
 */
trait Config {
  val fields: List[FieldDef[_]]

  /*! Some of the object we construct here might need this configuration instance */
  implicit val me: Config = this

  def cores: Option[Int] = None
  def limit: Option[Int] = None
  def windowSize = 10
  def threshold = 0.90

  private val mongoDb = MongoConnection()("pace")
  val source = new MongoDBSource(mongoDb("people"), new BSONAdapter)
  def collector: Collector = new MongoDBCollector(mongoDb("candidates"))

  def sortOn = "n"

  def ngramSize = 3
  def maxNgrams = 4

  def simhashRotationStep = 2
  def simhashAlgo: Simhash = new AdditiveSimhash()

  def scanner: Scanner = new SingleFieldScanner
  def duplicateDetector: Duplicates = new SortedNeighborhood

  def blockingPrefix = 4

  def progressStep = 1000
  def threadPoolBoost = 4
}

trait OverrideConfig extends Config {
  val conf = OptionalConfigFactory.load("conf/pace.conf")

  override def cores = conf.getInt("pace.cores")
  override def limit = conf.getInt("pace.limit")
  override def windowSize = conf.getInt("pace.windowSize").getOrElse(super.windowSize)
  override def threshold = conf.getDouble("pace.threshold").getOrElse(super.threshold)

  override def sortOn = conf.getString("pace.sortOn").getOrElse(super.sortOn)

  override def ngramSize = conf.getInt("pace.ngramSize").getOrElse(super.ngramSize)
  override def maxNgrams = conf.getInt("pace.maxNgrams").getOrElse(super.maxNgrams)


  override def simhashRotationStep = conf.getInt("pace.simhashRotationStep").getOrElse(super.simhashRotationStep)
  override def simhashAlgo = conf.getString("pace.simhash.algo") match {
    case Some("additive") => new AdditiveSimhash()
    case Some("balanced") => new BalancedSimhash()
    case Some("max") => new MaxSimhash(conf.getInt("pace.max.repeat").getOrElse(4))
    case None => super.simhashAlgo
  }

  override def scanner = conf.getString("pace.algo") match {
    case Some("singleField") => new SingleFieldScanner
    case Some("mergedSimhash") => new MergedSimhashScanner
    case Some("simhash") => new MultiPassSimhashScanner
    case Some("ngram") => new NgramScanner
    case None => super.scanner
  }

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
  override val fields= parseFields

  def parseFields: List[FieldDef[_]] = {
    import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._

    conf.getObject("pace.model") match {
      case Some(model) => {
        def parseField(name: String, cfg: ConfigValue) = {
          val weight = conf.getDouble("pace.model.%s.weight".format(name)).getOrElse(1.0)

          val algo = conf.getString("pace.model.%s.algo".format(name)) match {
            case Some("JaroWinkler") => JaroWinkler(_)
            case Some("Levenstein") => Levenstein(_)
            case Some("Null") => (_: Double) => NullDistanceAlgo()
            case None => (_: Double) => NullDistanceAlgo()
          }

          val field = conf.getString("pace.model.%s.type".format(name)) match {
            case Some("Int") => IntFieldDef(_, _)
            case Some("String") => StringFieldDef(_, _)
            case None => StringFieldDef(_, _)
          }

          field(name, algo(weight))
        }

        val res = for(name <- model.keySet)
                  yield parseField(name, model.get(name))
        res.toList
      }
      case None => List()
    }
  }
}
