addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"


addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addDependencyTreePlugin

//tag::scalaFix[]
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")
//end::scalaFix[]

//tag::sbtJNIPlugin[]
addSbtPlugin("com.github.sbt" %% "sbt-jni" % "1.5.4")
//end::sbtJNIPlugin[]

//tag::xmlVersionConflict[]
// See https://github.com/scala/bug/issues/12632
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
//end::xmlVersionConflict[]
