import demo.HashTagsStreaming.processTrendingHashTags
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.parsing.json.JSON
import scala.collection.immutable.Map

val jsonMap = JSON.parseFull(response).getOrElse(0).asInstanceOf[Map[String,String]]
val innerMap = jsonMap("result").asInstanceOf[Map[String,String]]


object TreatRentalData {

  def convertJson(jsonString: String ): Unit = {
    val jsonMap = JSON.parseFull(jsonString).getOrElse(0).asInstanceOf[Map[String,String]]
    val innerMap = jsonMap("result").asInstanceOf[Map[String,String]]
  }
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      System.err.println(
        """
          | Usage: TrendingHashtags <projectID> <windowLength> <slidingInterval> <totalRunningTime>
          |
          |     <projectID>: ID of Google Cloud project
          |     <windowLength>: The duration of the window, in seconds
          |     <slidingInterval>: The interval at which the window calculation is performed, in seconds
          |     <totalRunningTime>: Total running time for the application, in minutes. If 0, runs indefinitely until termination.
          |     <checkpointDirectory>: Directory used to store RDD checkpoint data
          |
        """.stripMargin)
      System.exit(1)
    }
    val ssc = new StreamingContext(sparkConf, 1)

    val sparkConf = new SparkConf().setAppName("TreatRentalData")
    val ssc = new StreamingContext(sparkConf, 1)
    val messagesStream: DStream[String] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        pubsub_subscription, // Cloud Pub/Sub subscription for incoming tweets
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8)

      val jsonData = messagesStream.forEachRDD(rdd => convertJson((rdd)))

  }
}