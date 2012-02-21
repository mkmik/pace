// Comment to get more information during initialization
logLevel := Level.Warn

resolvers ++= Seq(
    DefaultMavenRepository,
    Resolver.url("Play", url("http://download.playframework.org/ivy-releases/"))(Resolver.ivyStylePatterns),
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Scala Tools Nexus" at "https://oss.sonatype.org/content/groups/scala-tools/"
)


// Use the Play sbt plugin for Play projects
addSbtPlugin("play" % "sbt-plugin" % "2.0-RC2")

addSbtPlugin("com.github.philcali" % "sbt-cx-docco" % "0.1.0")
