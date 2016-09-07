addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"


resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

// Temporary hack for bintray being sad

resolvers +=  Resolver.bintrayRepo("jodersky", "sbt-jni-macros")
resolvers += "jodersky" at "https://dl.bintray.com/jodersky/maven/"

//tag::addSparkPackagesPlugin[]
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.2")
//end::addSparkPackagesPlugin[]

//addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.3")

addSbtPlugin("ch.jodersky" % "sbt-jni" % "1.0.0-RC3")
