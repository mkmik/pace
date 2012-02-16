import sbt._
import Keys._
import PlayProject._

object ApplicationBuild extends Build {

    val appName         = "pace"
    val appVersion      = "1.0"

    val appDependencies = Seq(
      // Add your project dependencies here,
      "com.mongodb.casbah" %% "casbah" % "2.1.5-1",
      "edu.cmu" % "secondstring" % "1.0.0-SNAPSHOT",
      "com.google.inject" % "guice" % "3.0",
      //"com.google.inject.extensions" % "guice-servlet" % "3.0",
      "uk.me.lings" % "scala-guice_2.8.0" % "0.1",

      "joda-time" % "joda-time" % "1.6.2",
      "postgresql" % "postgresql" % "8.4-701.jdbc4",
      "com.traveas" % "querulous-light_2.9.0" % "0.0.6",

      "com.jsuereth" %% "scala-arm" % "1.2",

      "net.sf.opencsv" % "opencsv" % "2.1",

      "com.typesafe.config" % "config" % "0.2.1"
    )

    val main = PlayProject(appName, appVersion, appDependencies).settings(defaultScalaSettings:_*).settings(
      // Add your own project settings here      
      resolvers += "Clojars" at "http://clojars.org/repo/",
      resolvers += "RI Releases" at "http://maven.research-infrastructures.eu/nexus/content/repositories/releases"
    )

}
