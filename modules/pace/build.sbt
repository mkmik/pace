publishTo := Some("RI Snapshots" at "http://maven.research-infrastructures.eu/nexus/content/repositories/snapshots/")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishMavenStyle := true
