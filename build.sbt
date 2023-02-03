organization := "com.highperformancespark"

//tag::addSparkScalaFix[]
ThisBuild / scalafixDependencies +=
  "com.holdenkarau" %% "spark-scalafix-rules-2.4.8" % "0.1.5"
ThisBuild / scalafixDependencies +=
  "com.github.liancheng" %% "organize-imports" % "0.6.0"
//end::addSparkScalaFix[]

lazy val V = _root_.scalafix.sbt.BuildInfo

scalaVersion := V.scala212
addCompilerPlugin(scalafixSemanticdb)
scalacOptions ++= List(
  "-Yrangepos",
  "-P:semanticdb:synthetics:on"
)


name := "examples"

publishMavenStyle := true

version := "0.0.1"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

parallelExecution in Test := false

fork := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-Djna.nosys=true")

Test / javaOptions ++= Seq(
  "base/java.lang", "base/java.lang.invoke", "base/java.lang.reflect", "base/java.io", "base/java.net", "base/java.nio",
  "base/java.util", "base/java.util.concurrent", "base/java.util.concurrent.atomic",
  "base/sun.nio.ch", "base/sun.nio.cs", "base/sun.security.action",
  "base/sun.util.calendar", "security.jgss/sun.security.krb5",
  ).map("--add-opens=java." + _ + "=ALL-UNNAMED")

val sparkVersion = settingKey[String]("Spark version")
val sparkTestingVersion = settingKey[String]("Spark testing base version without Spark version part")

// 2.4.5 is the highest version we have with the old spark-testing-base deps
sparkVersion := System.getProperty("sparkVersion", "3.3.0")
sparkTestingVersion := "1.4.0"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"                % sparkVersion.value,
  "org.apache.spark" %% "spark-streaming"           % sparkVersion.value,
  "org.apache.spark" %% "spark-sql"                 % sparkVersion.value,
  "org.apache.spark" %% "spark-hive"                % sparkVersion.value,
  "org.apache.spark" %% "spark-hive-thriftserver"   % sparkVersion.value,
  "org.apache.spark" %% "spark-catalyst"            % sparkVersion.value,
  "org.apache.spark" %% "spark-yarn"                % sparkVersion.value,
  "org.apache.spark" %% "spark-mllib"               % sparkVersion.value,
  "com.holdenkarau" %% "spark-testing-base"         % s"${sparkVersion.value}_${sparkTestingVersion.value}",
  //tag::scalaLogging[]
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  //end::scalaLogging[]
  "net.java.dev.jna" % "jna" % "5.12.1")


scalacOptions ++= Seq("-deprecation", "-unchecked")

pomIncludeRepository := { x => false }

resolvers ++= Seq(
  "JBoss Repository" at "https://repository.jboss.org/nexus/content/repositories/releases/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "https://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "https://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

// JNI

enablePlugins(JniNative)

sourceDirectory in nativeCompile := sourceDirectory.value

//tag::xmlVersionConflict[]
// See https://github.com/scala/bug/issues/12632
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
//end::xmlVersionConflict[]
