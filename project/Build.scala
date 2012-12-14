import sbt._
import Keys._

object ApplicationBuild extends Build {

  val libDependencies = Seq(
    // Add your project dependencies here,
    "edu.cmu" % "secondstring" % "1.0.0-SNAPSHOT",
    "com.google.inject" % "guice" % "3.0",
    //"com.google.inject.extensions" % "guice-servlet" % "3.0",
    "uk.me.lings" % "scala-guice_2.8.0" % "0.1",

    "joda-time" % "joda-time" % "2.0",
    "org.joda" % "joda-convert" % "1.2",
    //"postgresql" % "postgresql" % "8.4-701.jdbc4",
    //"com.traveas" % "querulous-light_2.9.0" % "0.0.6",

    "com.jsuereth" %% "scala-arm" % "1.2",
    "javax.transaction" % "jta" % "1.1" % "provided->default",
    //"javax.transaction" % "jta" % "1.1",

    "net.sf.opencsv" % "opencsv" % "2.1",

    "com.typesafe" % "config" % "0.3.1",

    "com.typesafe.akka" % "akka-remote" % "2.0.3",

    "org.scalatest" % "scalatest" % "1.2",

    "org.scalaj" %% "scalaj-time" % "0.6",

    "junit" % "junit" % "4.8.1" % "test",
    "org.specs2" %% "specs2" % "1.12.3" % "test",
    "eu.dnetlib" % "dnet-openaire-data-protos" % "1.0-SNAPSHOT" % "test",

    "org.json" % "json" % "20090211"
  )

  lazy val root = Project("pace", file(".")).settings(
    libraryDependencies := libDependencies,
    resolvers += "Clojars" at "http://clojars.org/repo/",
    resolvers += "RI Releases" at "http://maven.research-infrastructures.eu/nexus/content/repositories/releases",
    resolvers += "RI D-Net Bootstrap" at "http://maven.research-infrastructures.eu/nexus/content/repositories/dnet-bootstrap",
    resolvers += "RI D-Net Snapshots" at "http://maven.research-infrastructures.eu/nexus/content/repositories/dnet-snapshots",
    resolvers += "RI D-Net Deps" at "http://maven.research-infrastructures.eu/nexus/content/repositories/dnet-deps",
    resolvers += "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "Sonatyle releases"  at "http://oss.sonatype.org/content/repositories/releases"
  )
}
