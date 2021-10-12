// using scala 3
// using repository https://repository.apache.org/content/repositories/orgapachespark-1388
// using org.apache.spark:spark-core_2.13:3.2.0
// using org.apache.spark:spark-sql_2.13:3.2.0
// using io.github.vincenzobaz::spark-scala3:0.1.2+1-ad53fe95+20211012-1151-SNAPSHOT
// using org.scala-lang::scala3-tasty-inspector:3.0.2

import scala3encoders.given

import org.apache.spark.sql.SparkSession

case class Foo(a: String, b: Int)
case class Bar(b: Int, c: String)

object Hello {
  def getCatsTastyAndClasspath: (List[String], List[String]) = {
    val classpath = List(
      "/Users/fzybala/Documents/cats/kernel/.jvm/target/scala-3.0.2/classes",
      "/Users/fzybala/.ivy2/local/org.scala-lang/scala3-library_3/3.0.2/jars/scala3-library_3.jar",
      "/Users/fzybala/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/typelevel/simulacrum-scalafix-annotations_3/0.5.4/simulacrum-scalafix-annotations_3-0.5.4.jar",
      "/Users/fzybala/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala-library/2.13.6/scala-library-2.13.6.jar"
    )

    import java.io.File
    def recursiveListFiles(f: File): Array[File] = {
      val these = f.listFiles
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }

    val tastyFiles = List(
      File("/Users/fzybala/Documents/cats/core/.jvm/target/scala-3.0.2/classes/")
    ).flatMap(f => recursiveListFiles(f).toList).map(_.toString).filter(_.endsWith(".tasty"))

    (tastyFiles, classpath)
  }

  def main(args: Array[String]): Unit = {
    val (tastyFiles, classpath) = getCatsTastyAndClasspath
    val usages = MethodCollector.collect(tastyFiles, classpath)

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

    val usagesDF = usages.toDF()

    val reduced = usagesDF.groupBy("name").count().sort(col("count").desc)

    reduced.show(100, false)

    spark.stop()
  }
}