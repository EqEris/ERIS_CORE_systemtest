credentials += Credentials(Path.userHome / ".ivy2" / ".equeris_credentials")

resolvers ++= Seq(
  "Sonatype releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "jgit-repo"         at "http://download.eclipse.org/jgit/maven",
  "EqEris releases"   at "https://nexus.equeris.com/nexus/content/repositories/eqeris-releases-local/",
  Resolver.bintrayRepo("websudos", "oss-releases")
)

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.4")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.0")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.1.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-aspectj" % "0.10.0")
addSbtPlugin("com.eqeris" % "eris-sbt-plugin" % "1.0.1")