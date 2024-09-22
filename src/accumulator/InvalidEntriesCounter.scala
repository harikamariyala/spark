package accumulator

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object InvalidEntriesCounter {
  def main(args: Array[String]): Unit = {

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Invalid Entries Counter")
      .master("local[2]")
      .getOrCreate()

    // Create an accumulator
    val invalidEntries: LongAccumulator = spark.sparkContext.longAccumulator("Invalid Entries")

    // Sample data
    val data = spark.sparkContext.parallelize(Seq(
      "john@example.com",
      "jane.doe@acme.com",
      "invalid-email",
      "alice@example.com",
      "bob@",
      "carol@site"
    ))

    // Function to validate email
    def isValidEmail(email: String): Boolean = {
      email.contains("@") && email.contains(".")
    }

    // Use accumulator to count invalid emails
    data.foreach { email =>
      if (!isValidEmail(email)) {
        invalidEntries.add(1)
      }
    }

    // Output the result
    println(s"Number of invalid entries: ${invalidEntries.value}")

    // Stop the Spark session
    spark.stop()
  }
}