resolvers ++= Seq(
    DefaultMavenRepository,
    Resolver.url("Play", url("http://download.playframework.org/ivy-releases/"))(Resolver.ivyStylePatterns),
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies += "play" %% "play" % "2.0-beta"

addSbtPlugin("com.github.philcali" % "sbt-cx-docco" % "0.1.0")
