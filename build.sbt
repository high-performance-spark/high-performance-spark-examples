lazy val root = (project in file("."))
  .aggregate(core, native)


organization := "com.highperformancespark"

//tag::addSparkScalaFix[]
// Needs to be commented out post-upgrade because of Scala versions.
//ThisBuild / scalafixDependencies +=
//  "com.holdenkarau" %% "spark-scalafix-rules-2.4.8" % "0.1.5"
//ThisBuild / scalafixDependencies +=
//  "com.github.liancheng" %% "organize-imports" % "0.6.0"
//end::addSparkScalaFix[]

lazy val V = _root_.scalafix.sbt.BuildInfo

scalaVersion := "2.13.13"
addCompilerPlugin(scalafixSemanticdb)
scalacOptions ++= List(
  "-Yrangepos",
  "-P:semanticdb:synthetics:on"
)


name := "examples"

publishMavenStyle := true

version := "0.0.1"
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
  Resolver.sonatypeRepo("public"),
  Resolver.mavenLocal
)

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

def specialOptions = {
  // We only need these extra props for JRE>17
  if (sys.props("java.specification.version") > "1.17") {
    Seq(
      "base/java.lang", "base/java.lang.invoke", "base/java.lang.reflect", "base/java.io", "base/java.net", "base/java.nio",
      "base/java.util", "base/java.util.concurrent", "base/java.util.concurrent.atomic",
      "base/sun.nio.ch", "base/sun.nio.cs", "base/sun.security.action",
      "base/sun.util.calendar", "security.jgss/sun.security.krb5",
    ).map("--add-opens=java." + _ + "=ALL-UNNAMED")
  } else {
    Seq()
  }
}


val sparkVersion = settingKey[String]("Spark version")
val sparkTestingVersion = settingKey[String]("Spark testing base version without Spark version part")


// Core (non-JNI bits)

lazy val core = (project in file("core")) // regular scala code with @native methods
  .dependsOn(native % Runtime)
  .settings(javah / target := (native / nativeCompile / sourceDirectory).value / "include")
  .settings(scalaVersion := "2.13.13")
  .settings(sbtJniCoreScope := Compile)
  .settings(
    scalaVersion := "2.13.8",
    javacOptions ++= Seq("-source", "17", "-target", "17"),
    parallelExecution in Test := false,
    fork := true,
    javaOptions ++= Seq("-Xms4048M", "-Xmx4048M", "-Djna.nosys=true"),
    Test / javaOptions ++= specialOptions,
    // 2.4.5 is the highest version we have with the old spark-testing-base deps
    sparkVersion := System.getProperty("sparkVersion", "4.0.0-preview2"),
    sparkTestingVersion := "2.0.1",
    // additional libraries
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"                % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-streaming"           % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-sql"                 % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-hive"                % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-hive-thriftserver"   % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-catalyst"            % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-yarn"                % sparkVersion.value % Provided,
      "org.apache.spark" %% "spark-mllib"               % sparkVersion.value % Provided,
      "com.holdenkarau" %% "spark-testing-base"         % s"${sparkVersion.value}_${sparkTestingVersion.value}" % Test,
      //tag::scalaLogging[]
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
      //end::scalaLogging[]
      "net.java.dev.jna" % "jna" % "5.12.1"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    pomIncludeRepository := { x => false },
    resolvers += Resolver.mavenLocal
  )

// JNI Magic!
lazy val native = (project in file("native")) // native code and build script
  .settings(nativeCompile / sourceDirectory := sourceDirectory.value)
  .settings(scalaVersion := "2.13.13")
  .enablePlugins(JniNative) // JniNative needs to be explicitly enabled

//tag::xmlVersionConflict[]
// See https://github.com/scala/bug/issues/12632
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
//end::xmlVersionConflict[]

assemblyMergeStrategy in assembly := {
      case x => MergeStrategy.first
}

assemblyMergeStrategy in native := {
      case x => MergeStrategy.first
}

assemblyMergeStrategy in core := {
      case x => MergeStrategy.first
}
