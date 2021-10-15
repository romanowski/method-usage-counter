# Method Usage Counter

Library written in Scala 3 for counting method usages.

Library uses TASTy files as input. After collecting neccessary information from ASTs, it uses Spark to generate final results.

Run it with `scala-cli https://raw.githubusercontent.com/romanowski/types-usage-counter/master/main.scala`