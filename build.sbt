organization := "com.highperformancespark"

name := "spark-testing-base"

publishMavenStyle := true

version := "0.0.1"

scalaVersion := "2.10.4"

crossScalaVersions := Seq("2.10.4", "2.11.6")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

sparkVersion := "1.5.1"

spDependencies += "holdenk/spark-testing-base:0.1.3"

//tag::sparkComponents[]
sparkComponents ++= Seq("core", "streaming", "sql", "hive", "mllib")
//end::sparkComponents[]

parallelExecution in Test := false

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

// additional libraries
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1",
  "org.scalacheck" %% "scalacheck" % "1.12.4",
  "junit" % "junit" % "4.10",
  "org.eclipse.jetty" % "jetty-util" % "9.3.2.v20150730",
  "com.novocode" % "junit-interface" % "0.10" % "test->default")


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
  Resolver.sonatypeRepo("public")
)

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
