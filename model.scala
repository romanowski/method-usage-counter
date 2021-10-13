case class MethodUsage(name: String)

case class LibraryWrapper(org: String, name: String, version: String, tastyCp: String, classpath: String):
  def id = s"$org:$name:$version"