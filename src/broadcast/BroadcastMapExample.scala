package broadcast

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object BroadcastMapExample {
  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .appName("Broadcast Map Example")
      .master("local[*]") // Use local mode for example purposes
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    // Example data: Employees
    val employees = Seq(
      (1, "Alice", 101),
      (2, "Bob", 102),
      (3, "Charlie", 103),
      (4, "David", 101),
      (5, "Eve", 104)
    ).toDF("id", "name", "deptId")

    // Example data: Departments
    val departments = Seq(
      (101, "Finance"),
      (102, "Marketing"),
      (103, "IT"),
      (104, "HR")
    ).toDF("deptId", "deptName")

    // Convert departments DataFrame to a Map
    val deptMap: Map[Int, String] = departments
      .collect() // Collects data as an array of Rows
      .map(row => (row.getAs[Int]("deptId"), row.getAs[String]("deptName")))
      .toMap

    // Broadcast the Map
    val broadcastDeptMap: Broadcast[Map[Int, String]] = sc.broadcast(deptMap)

    // Use the broadcast Map for lookups
    val enrichedEmployees = employees.map(row => {
      val id = row.getAs[Int]("id")
      val name = row.getAs[String]("name")
      val deptId = row.getAs[Int]("deptId")
      val deptName = broadcastDeptMap.value.getOrElse(deptId, "Unknown")
      (id, name, deptId, deptName)
    }).toDF("id", "name", "deptId", "deptName")

    // Show the results
    enrichedEmployees.show()

    // Stop the Spark session
    spark.stop()
  }
}
