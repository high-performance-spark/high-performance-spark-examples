organization := "com.highperformancespark"

name := "examples"

publishMavenStyle := true

version := "0.0.1"

scalaVersion := "2.11.6"
scalaVersion in ThisBuild := "2.11.6"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

crossScalaVersions := Seq("2.11.6")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

//tag::sparkVersion[]
sparkVersion := "2.1.0"
//end::sparkVersion[]

//tag::sparkComponents[]
sparkComponents ++= Seq("core")
//end::sparkComponents[]
//tag::sparkExtraComponents[]
sparkComponents ++= Seq("streaming", "mllib")
//end::sparkExtraComponents[]
//tag::addSQLHiveComponent[]
sparkComponents ++= Seq("sql", "hive", "hive-thriftserver", "hive-thriftserver")
//end::addSQLHiveComponent[]

parallelExecution in Test := false

fork := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled", "-Djna.nosys=true")

// additional libraries
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1",
  "org.scalacheck" %% "scalacheck" % "1.13.4",
  "junit" % "junit" % "4.12",
  "junit" % "junit" % "4.11",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0",
  "com.novocode" % "junit-interface" % "0.11" % "test->default",
  //tag::scalaLogging[]
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  //end::scalaLogging[]
  "org.codehaus.jackson" % "jackson-core-asl" % "1.8.8",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.8",
  "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13",
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
  "net.java.dev.jna" % "jna" % "4.2.2")


scalacOptions ++= Seq("-deprecation", "-unchecked")

pomIncludeRepository := { x => false }

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public"),
  Resolver.bintrayRepo("jodersky", "sbt-jni-macros"),
  "jodersky" at "https://dl.bintray.com/jodersky/maven/"
)

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "log4j.properties" => MergeStrategy.discard
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}

// JNI

enablePlugins(JniNative)

sourceDirectory in nativeCompile := sourceDirectory.value
