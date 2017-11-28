import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
object Spark_analysis_2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local").appName("Spark Session example")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport().getOrCreate()
    val batchId = args(0)

    sparkSession.sqlContext.sql("USE project")
    sparkSession.sqlContext.sql("SELECT station_id from top_10_stations").show()
    sparkSession.sqlContext.sql("SELECT user_type,total_duration_in_minutes from song_duration").show()
    sparkSession.sqlContext.sql("SELECT artist_id from connected_artists").show()
    sparkSession.sqlContext.sql("SELECT song_id from top_10_songs_maxrevenue").show()
    sparkSession.sqlContext.sql("SELECT user_id from top_10_unsubscribed_users").show()


  }
}