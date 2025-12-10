import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.clustering.KMeans

object RfmBatch {

  // --- 1. Configuration ---
  // ⚠️ Ensure the CNET ID (peiyuanluo) in the path is correct
  val HDFS_INPUT_PATH = "/user/hadoop/peiyuanluo/finalProject/online_retail.csv"

  // Hive Table Names
  val HIVE_RFM_TABLE = "default.peiyuanluo_ecom_user_rfm_batch"
  val HIVE_SEGMENT_TABLE = "default.peiyuanluo_ecom_user_segments"

  val RFM_REFERENCE_DATE = "2011-12-10"
  val INVOICE_DATE_FORMAT = "d/M/yyyy H:mm"
  val K_CLUSTERS = 4

  def main(args: Array[String]): Unit = {

    // --- 2. Initialize Spark Session ---
    val spark = SparkSession.builder()
      .appName("EcomRFMBatchProcessingScala")
      .enableHiveSupport() // Enable Hive support to allow writing directly to Hive tables
      .getOrCreate()

    // Import implicits, key for Scala Spark programming, allowing usage of $ symbol and dataset operations
    import spark.implicits._

    // Set log level to reduce console noise
    spark.sparkContext.setLogLevel("WARN")

    println("--- [Step 1] Data Loading & Cleaning ---")

    // Define Schema
    val schema = StructType(Array(
      StructField("InvoiceNo", StringType, true),
      StructField("StockCode", StringType, true),
      StructField("Description", StringType, true),
      StructField("Quantity", IntegerType, true),
      StructField("InvoiceDate", StringType, true),
      StructField("UnitPrice", DoubleType, true),
      StructField("CustomerID", IntegerType, true),
      StructField("Country", StringType, true)
    ))

    // Read CSV Data
    val df = spark.read
      .option("header", "true")
      .option("encoding", "ISO-8859-1") // Handle special characters
      .schema(schema)
      .csv(HDFS_INPUT_PATH)

    // Data Cleaning: Filter out invalid quantity, unit price, and null Customer IDs
    val cleanedDf = df.filter(
      col("Quantity") > 0 && col("UnitPrice") > 0 && col("CustomerID").isNotNull
    )

    // Feature Engineering: Calculate TotalPrice and convert date format
    val featuresDf = cleanedDf
      .withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))
      .withColumn("InvoiceDateTimestamp", to_timestamp(col("InvoiceDate"), INVOICE_DATE_FORMAT))
      .filter(col("InvoiceDateTimestamp").isNotNull) // Filter out rows where date parsing failed

    println(s"Total records after cleaning: ${featuresDf.count()}")

    println("--- [Step 2] RFM Calculation ---")

    val referenceDate = lit(RFM_REFERENCE_DATE).cast(DateType)

    // ✅ Aggregate Calculation for R, F, M
    // Recency: Reference Date - Last Purchase Date
    // Frequency: Count distinct InvoiceNo (size + collect_set)
    // Monetary: Sum of Total Spending
    val rfmDf = featuresDf.groupBy("CustomerID").agg(
      datediff(referenceDate, max("InvoiceDateTimestamp").cast(DateType)).alias("Recency"),
      size(collect_set("InvoiceNo")).cast(LongType).alias("Frequency"),
      sum("TotalPrice").alias("Monetary")
    )

    println("Sample RFM Data (Top 5 by Monetary):")
    rfmDf.orderBy(desc("Monetary")).show(5)

    println(s"--- [Step 3] ML Pipeline (K-Means, K=$K_CLUSTERS) ---")

    // 3.1 Vector Assembler: Combine R, F, M columns into a single "features" vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("Recency", "Frequency", "Monetary"))
      .setOutputCol("features")

    val rfmAssembled = assembler.transform(rfmDf)

    // 3.2 Standard Scaler: Standardize features to prevent large monetary values from dominating clustering
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(rfmAssembled)
    val rfmScaled = scalerModel.transform(rfmAssembled)

    // 3.3 K-Means Training
    val kmeans = new KMeans()
      .setFeaturesCol("scaled_features")
      .setK(K_CLUSTERS)
      .setSeed(1L) // Set fixed seed for reproducibility

    val model = kmeans.fit(rfmScaled)

    // 3.4 Prediction: Generate SegmentID
    val predictions = model.transform(rfmScaled)

    // Extract results: CustomerID and SegmentID
    val userSegmentsDf = predictions.select(
      col("CustomerID"),
      col("prediction").alias("SegmentID")
    )

    println("Sample Segmentation Results:")
    userSegmentsDf.show(5)

    println("--- [Step 4] Write to Hive (Serving Layer) ---")

    // ⚠️ 4.1 Write RFM Table (Drop then Write)
    // Force DROP to resolve "Path does not exist" metadata conflict issues
    println(s"Dropping old table if exists: $HIVE_RFM_TABLE")
    spark.sql(s"DROP TABLE IF EXISTS $HIVE_RFM_TABLE")

    try {
      rfmDf.write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .saveAsTable(HIVE_RFM_TABLE)
      println(s"✅ Successfully wrote RFM table: $HIVE_RFM_TABLE")
    } catch {
      case e: Exception =>
        println(s"❌ Failed to write RFM table: ${e.getMessage}")
        // Catch exception but continue, or choose System.exit(1)
    }

    // ⚠️ 4.2 Write Segment Table (Drop then Write)
    println(s"Dropping old table if exists: $HIVE_SEGMENT_TABLE")
    spark.sql(s"DROP TABLE IF EXISTS $HIVE_SEGMENT_TABLE")

    try {
      userSegmentsDf.write
        .mode(SaveMode.Overwrite)
        .format("parquet")
        .saveAsTable(HIVE_SEGMENT_TABLE)
      println(s"✅ Successfully wrote Segment table: $HIVE_SEGMENT_TABLE")
    } catch {
      case e: Exception =>
        println(s"❌ Failed to write Segment table: ${e.getMessage}")
    }

    spark.stop()
  }
}