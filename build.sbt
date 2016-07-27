organization := "com.highperformancespark"

name := "examples"

publishMavenStyle := true

version := "0.0.1"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.11.6")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

sparkVersion := "2.0.0"

//tag::sparkComponents[]
sparkComponents ++= Seq("core", "streaming", "mllib")
//end::sparkComponents[]
//tag::addSQLHiveComponent[]
sparkComponents ++= Seq("sql", "hive", "hive-thriftserver", "hive-thriftserver")
//end::addSQLHiveComponent[]

parallelExecution in Test := false

fork := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled", "-Djna.nosys=true")

// additional libraries
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1",
  "org.scalacheck" %% "scalacheck" % "1.12.4",
  "junit" % "junit" % "4.10",
  //tag::sparkCSV[]
  "com.databricks" % "spark-csv_2.10" % "1.3.0",
  //end::sparkCSV[]
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.0.0-preview_0.4.1-preview",
  "org.eclipse.jetty" % "jetty-util" % "9.3.2.v20150730",
  "com.novocode" % "junit-interface" % "0.10" % "test->default",
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
  "jodersky" at "https://dl.bintray.com/jodersky/maven/",
  "spark-2.0-snapshot" at "https://repository.apache.org/content/repositories/orgapachespark-1182/"
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
