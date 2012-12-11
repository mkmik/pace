package pace

import afm.model.Document
import com.google.protobuf.GeneratedMessage

import scala.collection.JavaConversions._

trait Distance[A] {
  def toDocument(a: A): Document
}

class ProtoDistance extends Distance[GeneratedMessage] {
  def toDocument(a: GeneratedMessage): Document = {
    println("Scanning fields")
    traverse(a) { (path, value) =>
      println("At: %s, Value: %s".format(path, value))
    }
    null
  }

  def traverse(a: GeneratedMessage, path: Seq[String] = Seq(), level: Int = 0)(body: (String, String) => Unit): Unit = {
    for ((desc, v) <- a.getAllFields()) {
      //println("%sField '%s' '%s'".format(" " * level, desc.getName(), v.getClass))

      lazy val thisPath = desc.getName() +: path

      def stringValue(value: Object) = value match {
        case e: com.google.protobuf.Descriptors.EnumValueDescriptor => e.getName()
        case x => x.toString()
      }

      def down(v: Object) = v match {
        case v: GeneratedMessage => traverse(v, thisPath, level + 1)(body)
        case o => body(thisPath.reverse.mkString("."), stringValue(v))
      }

      if (desc.isRepeated()) {
        for (x <- v.asInstanceOf[java.util.List[Object]])
          down(x)
      } else {
        down(v)
      }
    }
  }
}
