// using scala 3
// using org.apache.spark:spark-core_2.13:3.2.0
// using org.apache.spark:spark-sql_2.13:3.2.0

// using repository https://wip-repos.s3.eu-central-1.amazonaws.com/.release
// using io.github.vincenzobaz::spark-scala3:0.1.3-new-spark

import scala3encoders.given 

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import java.util.Base64


case class SerializedTastyFile(lib: Library, path: String, contentBase64: String):
  def toTastyFile = TastyFile(lib, path, Base64.getDecoder().decode(contentBase64))
  //Base64.getEncoder().encodeToString(bytes)

object SerializedTastyFile:
  def from(tastyFile: TastyFile) =
    SerializedTastyFile(
      tastyFile.lib, 
      tastyFile.path, 
      Base64.getEncoder().encodeToString(tastyFile.content)
    )

case class SerializedTreeInfo(lib: Library, sourceFile: String, 
  method: String, treeKind: String, index: Int, depth: Int, topLevelType: String)

val NoType = "<no-type>"

def processLibraries(libs: Dataset[Library]): Dataset[SerializedTreeInfo] = 
  libs
    .flatMap(lib => loadTastyFiles(lib).map(SerializedTastyFile.from))
    .flatMap { serialized => 
      processTastyFile(serialized.toTastyFile) match
        case util.Left(msg) =>
          println(msg) // TODO better logging!
          Nil
        case util.Right(infos) =>
          infos.map( info =>
            SerializedTreeInfo(info.lib, info.sourceFile, info.method, info.treeKind, info.index, info.depth, info.topLevelType.getOrElse(NoType))
          )
    }

@main def localCluster = 
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


  val libs = Seq(
      Library("org.typelevel", "cats-core", "2.6.1"),
      Library("dev.zio", "zio", "2.0.0-M3"), 
      Library("org.http4s", "http4s-core", "1.0.0-M23"),
      Library( "co.fs2" , "fs2-core" , "3.1.5"), 
      Library( "org.tpolecat", "doobie-core" , "0.13.4"),
      Library( "io.monix" , "monix-eval" , "3.4.0")
  )

  val treeInfos = processLibraries(libs.toDF.as[Library])

  val counts = treeInfos
    .filter(col("topLevelType").=!=(NoType))
    .groupBy("topLevelType")
    .count()
    .sort(col("count").desc)

  counts.show(100, false)

  spark.stop()
