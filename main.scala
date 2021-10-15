// using scala 3
// using repository https://repository.apache.org/content/repositories/staging
// using org.apache.spark:spark-core_2.13:3.2.0
// using org.apache.spark:spark-sql_2.13:3.2.0

// using repository https://wip-repos.s3.eu-central-1.amazonaws.com/.release
// using io.github.vincenzobaz::spark-scala3:0.1.3-new-spark
// using tasty-query::tasty-query:0.0.2-spark

// using org.scala-lang::scala3-tasty-inspector:3.0.2
// using "io.get-coursier:coursier_2.13:2.0.16-169-g194ebc55c"
// using "org.scala-sbt:io_2.13:1.5.1"
// using com.lihaoyi::requests:0.6.9
// using com.lihaoyi::os-lib:0.7.8


import scala3encoders.given 

import org.apache.spark.sql.SparkSession
import coursier._
import sbt.io._
import sbt.io.syntax._
import java.io.File
import tastyquery.reader.TastyUnpickler
import tastyquery.Contexts
import tastyquery.api.ProjectReader
import tastyquery.ast.Trees.*
import tastyquery.ast.Types.*
import tastyquery.ast.Names.Name

case class Library(org: String, name: String, version: String)
case class TastyFile(lib: Library, path: String)(val content: Array[Byte])
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
      TastyFile(lib, relPath)(os.read.bytes(tastyDir / os.RelPath(relPath)))
    }
  finally os.remove.all(tmpDir)


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

def processTastyFile(tastyFile: TastyFile): Seq[TreeInfo] = 
  val unpickler = new TastyUnpickler(tastyFile.content)
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
      val tpe = t.tpeOpt.map {
        case named: NamedType => 
          named.name.toString + ":" + named.toString
        case other =>
          other.toString
      }
      val info = TreeInfo(
        tastyFile.lib, tastyFile.path, newMethod, t.getClass.getSimpleName,
        depth, newIndex, tpe
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
  

@main def tests =
  val libs = Seq(
      // Library("org.typelevel", "cats-core", "2.6.1"), // OK
      // Library("dev.zio", "zio", "2.0.0-M3"), // Fails with missing annotations
      // Library("org.typelevel", "cats-effect", "3.2.9"), // OK
      // Library("org.http4s", "http4s-core", "1.0.0-M23"), // Fails with NoSymbol
      // Library( "co.fs2" , "fs2-core" , "3.1.5"), // undefined: fs2.Stream.bracket # -1
      Library( "org.tpolecat", "doobie-core" , "0.13.4"), // Fails with undefined: fs2.Stream.Compiler.syncInstance
      // Library( "io.monix" , "monix-eval" , "3.4.0") // OK
  )
  println(libs.map(loadTastyFiles).flatMap(_.take(3)).flatMap(processTastyFile).filter(_.topLevelType.nonEmpty).take(25).mkString("\n"))
  

def app = 
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
