package pace

import afm.model.Document
import com.google.protobuf.GeneratedMessage
import scala.collection.JavaConversions._
import afm.model.Field
import afm.model.StringField
import afm.model.ListField
import afm.model.MapDocument
import afm.Config
import afm.distance.DistanceScorer

trait Distance[A] {
  implicit def toDocument(a: A): Document

  def between(a: A, b: A)(implicit config: Config): Double = new DistanceScorer(config.fields).distance(a, b)
}

class ProtoDistance(implicit config: Config) extends Distance[GeneratedMessage] {

  def toDocument(a: GeneratedMessage): Document = {
    var res = Map[String, Field[Any]]()

    println("Scanning fields")
    traverse(a) { (path, value) =>
      println("At: %s, Value: %s".format(path, value))
      res = res + (path -> (value match {
        case s: String => StringField(s)
        case l: List[_] => ListField(for (s <- l) yield StringField(s.asInstanceOf[String]))
      }))
    }

    new MapDocument(res)
  }

  def traverse(a: GeneratedMessage, path: Seq[String] = Seq(), level: Int = 0)(body: (String, Any) => Unit): Unit = {
    for ((desc, v) <- a.getAllFields()) {
      //println("%sField '%s' '%s'".format(" " * level, desc.getName(), v.getClass))

      lazy val thisPath = desc.getName() +: path

      def stringValue(value: Object) = value match {
        case e: com.google.protobuf.Descriptors.EnumValueDescriptor => e.getName()
        case x => x.toString()
      }

      def down(v: Object) = v match {
        case v: GeneratedMessage => traverse(v, thisPath, level + 1)(body)
        case o => body(thisPath.reverse.mkString(">"), stringValue(v))
      }

      if (desc.isRepeated()) {
        down((for (x <- v.asInstanceOf[java.util.List[Object]]) yield x).toList)
      } else {
        down(v)
      }
    }
  }
}
