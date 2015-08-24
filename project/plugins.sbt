// TODO this should not be here because its ide specific, just a workaround for the moment
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "3.0.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

