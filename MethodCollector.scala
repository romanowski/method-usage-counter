import scala.tasty.inspector.*
import scala.quoted.*
import scala.collection.mutable.ListBuffer
import java.io.File

object MethodCollector:
  class CustomTastyInspector extends Inspector:
    val buffer: ListBuffer[MethodUsage] = ListBuffer.empty
    override def inspect(using q: Quotes)(tastys: List[Tasty[quotes.type]]): Unit =
      import q.reflect.*

      class Traverser extends TreeAccumulator[List[MethodUsage]]:
        def foldTree(usages: List[MethodUsage], tree: Tree)(owner: Symbol): List[MethodUsage] =
          val usage: Option[MethodUsage] = Some(tree).flatMap {
            case s @ Select(term, name) if s.tpe.termSymbol.isDefDef => 
              Some(MethodUsage(s.tpe.termSymbol.fullName))
            case i @ Ident(name) if i.tpe.termSymbol.isDefDef => Some(MethodUsage(i.tpe.termSymbol.fullName))
            case _ => None
          }.filter(_.name.startsWith("scala")) // Leave methods from stdlib

          foldOverTree(usages ++ usage, tree)(tree.symbol)

      val traverser = Traverser()
      val usages = tastys.flatMap{ tasty => 
        traverser.foldTree(List.empty, tasty.ast)(tasty.ast.symbol)
      }
      buffer ++= usages

  def collect(libraryWrapper: LibraryWrapper): List[MethodUsage] = {
    val inspector = CustomTastyInspector()
    try 
      TastyInspector.inspectAllTastyFiles(
      libraryWrapper.tastyCp.split(File.pathSeparator).toList, 
      Nil,
      libraryWrapper.classpath.split(File.pathSeparator).toList)(inspector)
      inspector.buffer.toList
    catch 
      case e: Throwable =>
        println(s"FAILURE when processing: ${libraryWrapper.id}")
        e.printStackTrace
        Nil
  }
