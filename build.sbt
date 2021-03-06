// Rename this as you see fit
name := "thesis"

version := "0.0.1"

scalaVersion := "2.11.12"
//scalaVersion := "2.12.0"

organization := "com.ken"

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

logLevel := Level.Error

//fork := true
//fork := false
fork in run := true

connectInput in run := true

scalacOptions ++= Seq(
  "-nobootcp",
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials")

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }


resolvers ++= Seq(
  DefaultMavenRepository,
  "MavenRepository" at "http://central.maven.org/maven2",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "JBoss 3PP" at "https://repository.jboss.org/nexus/content/repositories/thirdparty-releases/",
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
  "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
  "Boundless Repository" at "http://repo.boundlessgeo.com/main/",
  "OSGeo Repository" at "http://download.osgeo.org/webdav/geotools/"
)

libraryDependencies ++= Seq(
  // Working combination
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "1.2.0-RC2",
  "org.locationtech.geotrellis" %% "geotrellis-proj4" % "1.2.1",
  "org.locationtech.geotrellis" %% "geotrellis-geotools" % "1.2.1",

  // Buffer underflow
//  "org.locationtech.geotrellis" %% "geotrellis-proj4" % "2.0.0-M1",
//  "org.locationtech.geotrellis" %% "geotrellis-spark" % "2.0.0-M1",
//  "org.locationtech.geotrellis" %% "geotrellis-geotools" % "2.0.0-M1",

//  "org.geotools" % "gt-shapefile" % "17.4",
  "org.geotools" % "gt-shapefile" % "19.0",
//  "org.geotools" % "gt-shapefile" % "19.0" % "provided",
//  "org.apache.spark"      %%  "spark-core"      % "2.2.0" % "provided",
  "org.apache.spark"      %%  "spark-core"      % "2.2.0",
  "org.scalatest"         %%  "scalatest"       % "2.2.0",
  "org.apache.hadoop" % "hadoop-client"         % "2.7.5",
  "com.lihaoyi" %% "pprint" % "0.4.3",
  //  "org.gdal" % "gdal" % "1.11.2"
  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "org.gdal" % "gdal" % "2.1.0",
  "com.github.pathikrit" %% "better-files" % "2.16.0",
//  "com.azavea.geotrellis" %% "geotrellis-gdal" % "0.10.0-M1"
  "ch.cern.sparkmeasure" %% "spark-measure" % "0.11"
)

// When creating fat jar, remote some files with
// bad signatures and resolve conflicts by taking the first
// versions of shared packaged types.
assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("org.opengis.filter.**" -> "opengisfilter.@1").inAll
//)

initialCommands in console := """
 |import geotrellis.raster._
 |import geotrellis.vector._
 |import geotrellis.proj4._
 |import geotrellis.spark._
 |import geotrellis.spark.io._
 |import geotrellis.spark.io.hadoop._
 |import geotrellis.spark.tiling._
 |import geotrellis.spark.util._
 """.stripMargin
