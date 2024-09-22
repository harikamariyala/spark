val kafkaVersion = "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0" // Adjust version as needed
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"  // Adjust version as needed
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.4.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion
