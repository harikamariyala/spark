package broadcast

import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset

object PreBroadcastedFiltersExample {
  def main(args: Array[String]): Unit = {

    // Initialize Spark
    val spark: SparkSession = SparkSession.builder()
      .appName("Pre-Broadcasted Filters Example")
      .master("local[*]") // Use local mode for example purposes
      .getOrCreate()
    import spark.implicits._

    // Example data: Transactions
    val transactions = Seq(
      (1, "12345", 100.0),
      (2, "67890", 150.0),
      (3, "12345", 50.0),
      (4, "54321", 300.0),
      (5, "98765", 200.0)
    ).toDF("id", "accountId", "amount")

    // Suspicious account IDs
    val suspiciousAccounts = Set("12345", "67890")
    val broadcastSuspiciousAccounts: Broadcast[Set[String]] = spark.sparkContext.broadcast(suspiciousAccounts)

    // Filter transactions using the broadcasted filter
    val filteredTransactions: Dataset[_] = transactions.filter(row => {
      broadcastSuspiciousAccounts.value.contains(row.getAs[String]("accountId"))
    })

    // Show the results
    filteredTransactions.show()

    // Stop the Spark session
    spark.stop()
  }
}