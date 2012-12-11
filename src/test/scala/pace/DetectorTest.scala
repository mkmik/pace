import org.specs2.mutable._
import resource._
import java.io._
import scala.sys.runtime
import afm._
import afm.util.Watch._
import afm.model._
import afm.feature._
import afm.duplicates._
import eu.dnetlib.data.proto.OafProtos.Oaf
import eu.dnetlib.data.proto.ResultProtos.Result
import eu.dnetlib.data.proto.OafProtos.OafEntity
import eu.dnetlib.data.proto.KindProtos
import eu.dnetlib.data.proto.TypeProtos
import eu.dnetlib.data.proto.InferenceProtos.Inference
import eu.dnetlib.data.proto.StructuredPropertyProtos.StructuredProperty
import eu.dnetlib.data.proto.QualifierProtos.Qualifier

object Builder {
  type B = Oaf.Builder

  def OafBuilder(a: B => B): B = a(Oaf.newBuilder.setInference(Inference.newBuilder))

  def EntityBuilder(a: OafEntity.Builder => OafEntity.Builder): B = OafBuilder(_.setKind(KindProtos.Kind.entity).setEntity(a(OafEntity.newBuilder())))

  def ResultBuilder(a: Result.Builder => Result.Builder): B = EntityBuilder(_.setType(TypeProtos.Type.result).setResult(a(Result.newBuilder())))

  def ResultMetadata(a: Result.Metadata.Builder => Result.Metadata.Builder): Result.Metadata.Builder = a(Result.Metadata.newBuilder().setResulttype(Qualifier.newBuilder))

  implicit def struct(value: String): StructuredProperty.Builder = StructuredProperty.newBuilder().setValue(value);
}

object DetectorSpec extends Specification {
  import Builder.struct

  "pace" should {
    "rule" in {

      val resA = Builder.ResultBuilder(_.setId("A").setMetadata(Builder.ResultMetadata(_.addTitle("Test")))).build;
      val resB = Builder.ResultBuilder(_.setId("B").setMetadata(Builder.ResultMetadata(_.addTitle("Tost")))).build;

      println("resA", resA)
      println("resB", resB)
    }
  }
}
