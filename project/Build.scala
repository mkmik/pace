import sbt._
import Keys._
import PlayProject._

object ApplicationBuild extends Build {

    val appName         = "testapp"
    val appVersion      = "1.0"

    val appDependencies = Seq(
      // Add your project dependencies here,
      "com.mongodb.casbah" %% "casbah" % "2.1.5-1",
      "edu.cmu" % "secondstring" % "1.0.0-SNAPSHOT"
    )

    val main = PlayProject(appName, appVersion, appDependencies).settings(defaultScalaSettings:_*).settings(
      // Add your own project settings here      
      resolvers += "Clojars" at "http://clojars.org/repo/"
    )

}
