// using scala 3
// using repository https://repository.apache.org/content/repositories/staging
// using org.apache.spark:spark-core_2.13:3.2.0
// using org.apache.spark:spark-sql_2.13:3.2.0

// using repository https://wip-repos.s3.eu-central-1.amazonaws.com/.release
// using io.github.vincenzobaz::spark-scala3:0.1.3-new-spark

// using org.scala-lang::scala3-tasty-inspector:3.0.2
// using "io.get-coursier:coursier_2.13:2.0.16-169-g194ebc55c"
// using "org.scala-sbt:io_2.13:1.5.1"

import scala3encoders.given

import org.apache.spark.sql.SparkSession
import coursier._
import sbt.io._
import sbt.io.syntax._
import java.io.File

def fetch(tempDir: File)(org: String, name: String, version: String, prefix: String): LibraryWrapper = 
  import coursier.*
  val params = coursier.params.ResolutionParams().withScalaVersion("3.0.2")
  val dep = Dependency(Module(Organization(org), ModuleName(name)), version)
  val result = Fetch().addDependencies(dep).withResolutionParams(params).runResult()
  val jars = result.detailedArtifacts.filter(_._2.`type` == Type.jar)
  val aDep = jars.filter { case (dep, _, _, _) => 
    dep.module.name.value.startsWith(prefix) && dep.module.organization.value == org  
  }

  val dest = tempDir / s"$org-$name-$version"
  IO.createDirectory(dest)

  aDep.foreach(d => IO.unzip(d._4, dest, ExtensionFilter("tasty")))
  val tastyFiles = (dest ** "*").filter( ! _.isDirectory).get.map(_.toString)

  // Tasty reader is broken and we need to provide both .class files and .tasty files now
  LibraryWrapper(
    org,
    name,
    version,
    tastyFiles.mkString(File.pathSeparator), // TODO move resolution locally to node
    jars.map(_._4.toString).mkString(File.pathSeparator)
  )

@main def app = 
  val baseFile = IO.createTemporaryDirectory
  try {
    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }

    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val fetchToTemp = fetch(baseFile)

    val librariesDF = List(
      fetchToTemp("org.typelevel", "cats-core_3", "2.6.1", "cats"), // OK
      // fetchToTemp("dev.zio", "zio_3", "2.0.0-M3", "zio"), // Fails with missing annotations
      fetchToTemp("org.typelevel", "cats-effect_3", "3.2.9", "cats-effect"), // OK
      // fetchToTemp("org.http4s", "http4s-core_3", "1.0.0-M23", "http4s") // Fails with NoSymbol
      // fetchToTemp( "co.fs2" , "fs2-core_3" , "3.1.5", "fs2"), // undefined: fs2.Stream.bracket # -1
      // fetchToTemp( "org.tpolecat" , "doobie-core_3" , "0.13.4", "doobie"), // Fails with undefined: fs2.Stream.Compiler.syncInstance
      fetchToTemp( "io.monix" , "monix-eval_3" , "3.4.0", "monix") // OK
    ).toDF
      
    val usagesDF = librariesDF.as[LibraryWrapper].flatMap(MethodCollector.collect)

    val reduced = usagesDF.groupBy("name").count().sort(col("count").desc)

    reduced.show(100, false)

    spark.stop()
  } finally IO.delete(baseFile)
