// using scala 3
// using repository https://repository.apache.org/content/repositories/staging
// using org.apache.spark:spark-core_2.13:3.2.0
// using org.apache.spark:spark-sql_2.13:3.2.0

// using repository https://wip-repos.s3.eu-central-1.amazonaws.com/.release
// using io.github.vincenzobaz::spark-scala3:0.1.3-new-spark
// using tasty-query::tasty-query:0.0.2-spark

// using org.scala-lang::scala3-tasty-inspector:3.0.2
// using "org.scala-sbt:io_2.13:1.5.1"
// using com.lihaoyi::requests:0.6.9
// using com.lihaoyi::os-lib:0.7.8

import scala3encoders.given 
import scala3encoders.derivation.Serializer
import scala3encoders.derivation.Deserializer

import org.apache.spark.sql.SparkSession
import sbt.io._
import sbt.io.syntax._
import java.io.File
import tastyquery.reader.TastyUnpickler
import tastyquery.Contexts
import tastyquery.api.ProjectReader
import tastyquery.ast.Trees.*
import tastyquery.ast.Types.*
import tastyquery.ast.Names.Name
import java.util.Base64

case class Library(org: String, name: String, version: String)
case class TastyFile(lib: Library, path: String, val contentBase64: String)
case class TreeInfo(lib: Library, sourceFile: String, 
  method: String, kind: String, index: Int, depth: Int, topLevelType: Option[String])


def loadTastyFiles(lib: Library): Seq[TastyFile] = 
  // TODO do it in memory!
  val tmpDir = os.temp.dir()
  try 
    val zipFile = tmpDir / "lib.zip"
    val orgPath = lib.org.split('.').mkString("/")
    val address = 
      s"https://repo1.maven.org/maven2/$orgPath/${lib.name}_3/${lib.version}/${lib.name}_3-${lib.version}.jar"
    os.write(
      zipFile,
      requests.get.stream(address)
    )
    val tastyDir = tmpDir / "tasty"
    IO.unzip(zipFile.wrapped.toFile, tastyDir.wrapped.toFile, ExtensionFilter("tasty"))
    val tastyFiles = (tastyDir.wrapped.toFile ** "*").filter( ! _.isDirectory).get
    tastyFiles.toList.map { file =>
      val relPath = tastyDir.wrapped.relativize(file.toPath).toString
      val bytes = os.read.bytes(tastyDir / os.RelPath(relPath))
      TastyFile(lib, relPath, Base64.getEncoder().encodeToString(bytes))
    }
  finally os.remove.all(tmpDir)


def printType(tpe: Type): String = tpe match
   case PackageRef(pck) => pck.toString
   case TypeRef(base, name) => printType(base) + "." + name.toString
   case ThisType(base) => printType(base) + ".this"
   case TermRef(base,name) => printType(base) + "." + name.toString
   case AppliedType(base, args) => printType(base) + args.map(_ => "_").mkString("[", ", ", "]")
   case _ => "TODO:" + tpe.toString

def isInteresting(tpe: Type): Boolean = tpe match
  case _: PackageRef => false
  case _ => true

def processTastyFile(tastyFile: TastyFile): Seq[TreeInfo] = 
  try
    val content =  Base64.getDecoder().decode(tastyFile.contentBase64)
    val unpickler = new TastyUnpickler(content)
    val ast = 
      unpickler.unpickle(new TastyUnpickler.TreeSectionUnpickler()).get.unpickle(using Contexts.empty(tastyFile.path))

    def walk(t: Tree, index: Int, depth: Int, method: String, topLevel: Boolean): (Int, Seq[TreeInfo]) =
      def processChildren(
        newMethod: String = method,
        nIndex: Int = index, 
        trees: Seq[TreeInfo] = Nil, 
        newTopLevel: Boolean = topLevel
        ) =
          t.subtrees.foldLeft((nIndex, trees)){
            case ((index, trees), tree) =>
              val (nIndex, newTrees) = walk(tree, index, depth + 1, newMethod, topLevel)
              (nIndex, trees ++ newTrees)
          }

      def processTreeAndChildren(newMethod: String = method) =
        val newIndex = index + 1
        val info = TreeInfo(
          tastyFile.lib, tastyFile.path, newMethod, t.getClass.getSimpleName,
          depth, newIndex, t.tpeOpt.filter(isInteresting).map(printType)
        )
        processChildren(newMethod, index + 1, Seq(info))

      def ignore = (index, Nil)

      def newMethod(name: Name) = 
        if method.isEmpty then name.toString else s"$method.$name"

      t match
        case PackageDef(pid, stats)                   => processChildren(newMethod(pid.name))
        case ImportSelector(imported, renamed, bound) => ignore
        case Import(expr, selectors)                  => ignore
        case Class(name, rhs, symbol)                 => processChildren(newMethod(name))
        case _: Template | _: Block                   => processChildren()
        case ValDef(name, tpt, rhs, symbol)           => processChildren(newMethod(name), newTopLevel = false)
        case DefDef(name, params, tpt, rhs, symbol)   => processChildren(newMethod(name), newTopLevel = false)
        case _                                        => processTreeAndChildren()

    ast.flatMap(walk(_ , 0, 0, "", topLevel = true)._2)
  catch 
    case e: Throwable => // Ugly AF
      // e.printStackTrace
      Nil

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


    val libs = Seq(
        Library("org.typelevel", "cats-core", "2.6.1"),
        Library("dev.zio", "zio", "2.0.0-M3"), 
        Library("org.http4s", "http4s-core", "1.0.0-M23"),
        Library( "co.fs2" , "fs2-core" , "3.1.5"), 
        Library( "org.tpolecat", "doobie-core" , "0.13.4"),
        Library( "io.monix" , "monix-eval" , "3.4.0")
    )
      
    val usagesDF = libs
      .toDF
      .as[Library]
      .flatMap(loadTastyFiles)
      //.as[TastyFile]
      .flatMap(processTastyFile)
      // .filter(_.topLevelType.exists(_.startsWith("TODO:"))) // does not work with
            //       Exception in thread main: org.apache.spark.sql.AnalysisException: cannot resolve 'wrapoption(topLevelType.toString, StringType)' due to data type mismatch: argument 1 requires string type, however, 'topLevelType.toString' is of java.lang.String type.;
            // 'TypedFilter main$package$$$Lambda$1358/0x0000000800c02040@f511a8e, class TreeInfo, [StructField(lib,Seq(StructField(org,StringType,true), StructField(name,StringType,true), StructField(version,StringType,true)),true), StructField(sourceFile,StringType,true), StructField(method,StringType,true), StructField(kind,StringType,true), StructField(index,IntegerType,true), StructField(depth,IntegerType,true), StructField(topLevelType,StringType,true)], newInstance(class TreeInfo)
            // +- SerializeFromObject [if (isnull(knownnotnull(assertnotnull(input[0, TreeInfo, true])).lib)) null else named_struct(org, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, TreeInfo, true])).lib).org, true, false), name, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, TreeInfo, true])).lib).name, true, false), version, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, TreeInfo, true])).lib).version, true, false)) AS lib#37, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, TreeInfo, true])).sourceFile, true, false) AS sourceFile#38, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, TreeInfo, true])).method, true, false) AS method#39, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, TreeInfo, true])).kind, true, false) AS kind#40, knownnotnull(assertnotnull(input[0, TreeInfo, true])).index AS index#41, knownnotnull(assertnotnull(input[0, TreeInfo, true])).depth AS depth#42, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, unwrapoption(ObjectType(class java.lang.String), knownnotnull(assertnotnull(input[0, TreeInfo, true])).topLevelType), true, false) AS topLevelType#43]
            //    +- MapPartitions org.apache.spark.sql.Dataset$$Lambda$1340/0x0000000800bed840@5e9db5e7, obj#36: TreeInfo
            //       +- DeserializeToObject newInstance(class TastyFile), obj#35: TastyFile
            //          +- SerializeFromObject [if (isnull(knownnotnull(assertnotnull(input[0, TastyFile, true])).lib)) null else named_struct(org, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, TastyFile, true])).lib).org, true, false), name, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, TastyFile, true])).lib).name, true, false), version, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(knownnotnull(assertnotnull(input[0, TastyFile, true])).lib).version, true, false)) AS lib#25, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, TastyFile, true])).path, true, false) AS path#26, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, knownnotnull(assertnotnull(input[0, TastyFile, true])).contentBase64, true, false) AS contentBase64#27]
            //             +- MapPartitions org.apache.spark.sql.Dataset$$Lambda$1340/0x0000000800bed840@5fd8302e, obj#24: TastyFile
            //                +- DeserializeToObject newInstance(class Library), obj#23: Library
            //                   +- LocalRelation [org#3, name#4, version#5]

    val counts = usagesDF
      // Exception in thread main: org.apache.spark.sql.AnalysisException: cannot resolve 'wrapoption(topLevelType.toString, StringType)' due to data type mismatch: argument 1 requires string type, however, 'topLevelType.toString' is of java.lang.String type.
      // .filter(col("topLevelType").startsWith("TODO")) 
      .groupBy("topLevelType")
      .count()
      .sort(col("count").desc)

    counts.show(1000, false)

    spark.stop()
  } finally IO.delete(baseFile)
