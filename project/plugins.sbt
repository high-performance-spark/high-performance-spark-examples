resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"


addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.0")

addDependencyTreePlugin

//tag::scalaFix[]
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.4")
//end::scalaFix[]

//tag::sbtJNIPlugin[]
addSbtPlugin("com.github.sbt" %% "sbt-jni" % "1.7.1")
//end::sbtJNIPlugin[]

//tag::xmlVersionConflict[]
// See https://github.com/scala/bug/issues/12632
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
//end::xmlVersionConflict[]

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.5")
