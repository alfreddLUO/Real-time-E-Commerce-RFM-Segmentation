import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object RfmStreaming {

  // --- AWS MSK Configuration ---
  // 1. MSK Broker Address (Public)
  val KAFKA_BOOTSTRAP_SERVERS = "boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196"

  // 2. Topic Name
  val KAFKA_TOPIC = "peiyuanluo_orders"

  // 3. Hive Table
  val HIVE_SEGMENT_TABLE = "default.peiyuanluo_ecom_user_segments"

  // 4. Security Credentials (SASL/SCRAM)
  val USERNAME = "mpcs53014-2025"
  // ⚠️⚠️⚠️ Fill in the password retrieved via cat /home/hadoop/kafka.client.properties
  val PASSWORD = "A3v4rd4@ujjw"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("EcomRFMStreamingScala")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    println(s"--- Starting Spark Structured Streaming (Connecting to MSK: $USERNAME) ---")

    // Build JAAS Config String
    // Note: AWS MSK typically uses ScramLoginModule
    val jaasConfig = s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$USERNAME" password="$PASSWORD";"""

    // 1. Read Kafka Stream (With Security Configuration)
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      // --- Security Configuration Start ---
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512") // AWS MSK default is usually 512
      .option("kafka.sasl.jaas.config", jaasConfig)
      // .option("kafka.ssl.truststore.location", "/path/to/truststore") // Only needed for self-signed certs; not needed for AWS public certs
      // --- Security Configuration End ---
      .load()

    // 2. Define Schema
    val schema = new StructType()
      .add("CustomerID", IntegerType)
      .add("InvoiceNo", StringType)
      .add("Quantity", IntegerType)
      .add("UnitPrice", DoubleType)
      .add("Country", StringType)
      .add("InvoiceDate", StringType)

    // 3. Parse and Process
    val ordersDf = df.selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("data"))
      .select("data.*")
      .withColumn("TotalPrice", $"Quantity" * $"UnitPrice")
      .filter($"CustomerID".isNotNull)

    // 4. Load Static Hive Table
    val segmentsDf = spark.table(HIVE_SEGMENT_TABLE)

    // 5. Join
    val enrichedDf = ordersDf.join(segmentsDf, "CustomerID")

    // 6. Output to Console
    val query = enrichedDf.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination()
  }
}