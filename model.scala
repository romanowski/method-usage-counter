case class MethodUsage(name: String)

case class LibraryWrapper(tastyFiles: List[String], classpath: List[String]):
    import LibraryWrapper._
    def serialized: LibraryWrapper.Serialized = LibraryWrapper.Serialized(tastyFiles.mkString(sep), classpath.mkString(sep))

object LibraryWrapper:
    private val sep = System.getProperty("path.separator")
    case class Serialized(tastyFiles: String, classpath: String):
        def deserialized: LibraryWrapper = LibraryWrapper(tastyFiles.split(sep).toList, classpath.split(sep).toList)