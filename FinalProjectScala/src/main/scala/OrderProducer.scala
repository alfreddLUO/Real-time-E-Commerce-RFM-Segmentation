import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats
import org.json4s.native.Serialization.write
import scala.util.Random

// Define order data structure
case class Order(CustomerID: Int, InvoiceNo: String, Quantity: Int, UnitPrice: Double, Country: String, InvoiceDate: String)

object OrderProducer {

  // --- Configuration Parameters ---
  val BOOTSTRAP_SERVERS = "boot-public-byg.mpcs53014kafka.2siu49.c2.kafka.us-east-1.amazonaws.com:9196"
  val TOPIC = "peiyuanluo_orders"
  val USERNAME = "mpcs53014-2025"
  val PASSWORD = "A3v4rd4@ujjw"

  def main(args: Array[String]): Unit = {
    println(s"🚀 Starting Scala Producer. Target: $TOPIC")

    // 1. Set Kafka Properties
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // --- AWS MSK Security Configuration ---
    props.put("security.protocol", "SASL_SSL")
    props.put("sasl.mechanism", "SCRAM-SHA-512")
    val jaasConfig = s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$USERNAME" password="$PASSWORD";"""
    props.put("sasl.jaas.config", jaasConfig)

    // 2. Create Producer
    val producer = new KafkaProducer[String, String](props)
    implicit val formats: DefaultFormats.type = DefaultFormats
    val random = new Random()
    val customerIds = Seq(12347, 12583, 17850, 15502, 12345)

    try {
      while (true) {
        // 3. Generate Mock Data
        val order = Order(
          CustomerID = customerIds(random.nextInt(customerIds.length)),
          InvoiceNo = s"INV-${System.currentTimeMillis()}",
          Quantity = random.nextInt(10) + 1,
          UnitPrice = BigDecimal(random.nextDouble() * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
          Country = "USA",
          InvoiceDate = java.time.LocalDateTime.now().toString
        )

        // 4. Serialize to JSON
        val jsonString = write(order)

        // 5. Send
        val record = new ProducerRecord[String, String](TOPIC, order.CustomerID.toString, jsonString)
        producer.send(record)

        println(s"✅ Sent: $jsonString")

        Thread.sleep(2000) // Send every 2 seconds
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      producer.close()
    }
  }
}