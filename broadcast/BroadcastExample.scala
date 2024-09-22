package broadcast

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BroadcastExample {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Broadcast Example with RDD")
      .master("local[*]") // Use local mode for example purposes
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    // Example data: Employee IDs
    val employeeIds: RDD[Int] = sc.parallelize(Seq(101, 102, 103, 104, 105))

    // Large lookup table: Employee ID to Department Name
    val deptLookup: Map[Int, String] = Map(101 -> "Finance", 102 -> "Marketing", 103 -> "IT", 104 -> "HR", 105 -> "Operations")

    // Create a broadcast variable
    val broadcastDeptLookup = sc.broadcast(deptLookup)

    // Use the broadcast variable to enrich the employee IDs with department information
    val enrichedEmployeeData: RDD[(Int, String)] = employeeIds.map(id => (id, broadcastDeptLookup.value.getOrElse(id, "Unknown")))

    // Collect and print the results
    enrichedEmployeeData.collect().foreach(println)

    // Stop the Spark context
    sc.stop()
  }
}
