name := "UIETNSBootcamp"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
libraryDependencies += "com.google.code.gson" % "gson" % "1.7.1"
libraryDependencies += "org.apache.spark"%% "spark-mllib"% "1.6.2" withSources()
libraryDependencies += "org.apache.spark"%% "spark-sql" % "1.6.2" withSources()
    