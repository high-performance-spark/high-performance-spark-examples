addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"


addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.9.2")

addDependencyTreePlugin

//tag::scalaFix[]
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.12.1")
//end::scalaFix[]

//tag::sbtJNIPlugin[]
addSbtPlugin("com.github.sbt" %% "sbt-jni" % "1.7.0")
//end::sbtJNIPlugin[]

//tag::xmlVersionConflict[]
// See https://github.com/scala/bug/issues/12632
ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)
//end::xmlVersionConflict[]

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
