import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.spark.sql.SparkSession

object Spark_analysis {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("Data Analysis Main_1")
      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
      .config("hive.metastore.uris","thrift://127.0.0.1:9083")
      .enableHiveSupport()
      .getOrCreate()

    val batchId = args(0)

    //<<<<<<<<<---------- PROBLEM 1 - Creation of table and Insertion of data ------------>>>>>>>>>>>>
    //Determine top 10 station_id(s) where maximum number of songs were played, which were liked by unique users.

    val set_properties = sparkSession.sqlContext.sql("set hive.auto.convert.join=false")

    val use_project_database = sparkSession.sqlContext.sql("USE project")

    val create_hive_table_top_10_stations = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.top_10_stations"+
      "("+
      " station_id STRING,"+
      " total_distinct_songs_played INT,"+
      " distinct_user_count INT"+
      ")"+
      " PARTITIONED BY (batchid INT)"+
      " ROW FORMAT DELIMITED"+
      " FIELDS TERMINATED BY ','"+
      " STORED AS TEXTFILE")


    val insert_into_top_10_stations = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.top_10_stations"+
      s" PARTITION (batchid=$batchId)"+
      " SELECT"+
      " station_id,"+
      " COUNT(DISTINCT song_id) AS total_distinct_songs_played,"+
      " COUNT(DISTINCT user_id) AS distinct_user_count"+
      " FROM project.enriched_data"+
      " WHERE status='pass'"+
      s" AND (batchid=$batchId)"+
      " AND like=1"+
      " GROUP BY station_id"+
      " ORDER BY total_distinct_songs_played DESC"+
      " LIMIT 10")

    //<<<<<<<<<---------- PROBLEM 2 - Creation of table and Insertion of data ------------>>>>>>>>>>>>
    /*Determine total duration of songs played by each type of user, where type of user can be 'subscribed' or 'unsubscribed'.
    An unsubscribed user is the one whose record is either not present in Subscribed_users lookup table or has subscription_end_date
    earlier than the timestamp of the song played by him.*/

    val create_hive_table_song_duration = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.song_duration"+
      "("+
      " user_id STRING,"+
      " user_type STRING,"+
      " song_id STRING,"+
      " artist_id STRING,"+
      " total_duration_in_minutes DOUBLE"+
      ")"+
      " PARTITIONED BY (batchid INT)"+
      " ROW FORMAT DELIMITED"+
      " FIELDS TERMINATED BY ','"+
      " STORED AS TEXTFILE")


    val insert_into_song_duration = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.song_duration"+
      s" PARTITION (batchid=$batchId)"+
      " SELECT"+
      " e.user_id STRING,"+
      " IF(e.user_id!=s.user_id"+
      " OR (CAST(s.subscn_end_dt as BIGINT) < CAST(e.start_ts as BIGINT)),'unsubscribed','subscribed') AS user_type,"+
      " e.song_id STRING,"+
      " e.artist_id STRING,"+
      " (cast(e.end_ts as BIGINT)-cast(e.start_ts as BIGINT))/60 AS total_duration_in_minutes"+
      " FROM project.enriched_data e"+
      " LEFT OUTER JOIN project.subscribed_users s"+
      " ON e.user_id=s.user_id"+
      " WHERE e.status='pass'"+
      s" AND (batchid=$batchId)")


    //<<<<<<<<<---------- PROBLEM 3 - Creation of table and Insertion of data ------------>>>>>>>>>>>>
    //Determine top 10 connected artists.
    //Connected artists are those whose songs are most listened by the unique users who follow them.

    val create_hive_table_top_10_connected_artists = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.connected_artists"+
      "("+
      " artist_id STRING,"+
      " total_distinct_songs INT,"+
      " unique_followers INT"+
      ")"+
      " PARTITIONED BY (batchid INT)"+
      " ROW FORMAT DELIMITED"+
      " FIELDS TERMINATED BY ','"+
      " STORED AS TEXTFILE")


    val insert_into_top_10_connected_artists = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.connected_artists"+
      s" PARTITION (batchid=$batchId)"+
      " SELECT"+
      " artist_id,"+
      " COUNT(DISTINCT song_id) AS total_distinct_songs,"+
      " COUNT(DISTINCT user_id) AS unique_followers"+
      " FROM project.enriched_data"+
      " WHERE status='pass'"+
      s" AND (batchid=$batchId)"+
      " GROUP BY artist_id"+
      " ORDER BY unique_followers desc,total_distinct_songs desc"+
      " LIMIT 10")


    //<<<<<<<<<---------- PROBLEM 4 - Creation of table and Insertion of data ------------>>>>>>>>>>>>
    //Determine top 10 songs who have generated the maximum revenue.
    //NOTE: Royalty applies to a song only if it was liked or was completed successfully or both.

    val create_hive_table_top_10_songs_maxrevenue = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.top_10_songs_maxrevenue"+
      "("+
      " song_id STRING,"+
      " artist_id STRING,"+
      " total_duration_in_minutes DOUBLE"+
      " )"+
      " PARTITIONED BY (batchid INT)"+
      " ROW FORMAT DELIMITED"+
      " FIELDS TERMINATED BY ','"+
      " STORED AS TEXTFILE")


    val insert_into_top_10_songs_maxrevenue = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.top_10_songs_maxrevenue"+
      s" PARTITION (batchid=$batchId)"+
      " SELECT"+
      " song_id,"+
      " artist_id,"+
      " (cast(end_ts as BIGINT)-cast(start_ts as BIGINT))/60 AS total_duration_in_minutes"+
      " FROM project.enriched_data"+
      " WHERE status='pass'" +
      s" AND (batchid=$batchId)"+
      " AND (like=1 OR song_end_type=0 OR (like=1 and song_end_type=0))"+
      " ORDER BY total_duration_in_minutes desc"+
      " LIMIT 10")


    //<<<<<<<<<---------- PROBLEM 5 - Creation of table and Insertion of data ------------>>>>>>>>>>>>
    //Determine top 10 unsubscribed users who listened to the songs for the longest duration.

    val create_hive_table_top_10_unsubscribed_users = sparkSession.sqlContext.sql("CREATE TABLE IF NOT EXISTS project.top_10_unsubscribed_users"+
      "("+
      " user_id STRING,"+
      " song_id STRING,"+
      " artist_id STRING,"+
      " total_duration_in_minutes DOUBLE"+
      ")"+
      " PARTITIONED BY (batchid INT)"+
      " ROW FORMAT DELIMITED"+
      " FIELDS TERMINATED BY ','"+
      " STORED AS TEXTFILE")


    val insert_into_unsubscribed_users = sparkSession.sqlContext.sql("INSERT OVERWRITE TABLE project.top_10_unsubscribed_users"+
      s" PARTITION (batchid=$batchId)"+
      " SELECT"+
      " user_id,"+
      " song_id,"+
      " artist_id,"+
      " total_duration_in_minutes"+
      " FROM project.song_duration"+
      " WHERE user_type='unsubscribed'"+
      " AND total_duration_in_minutes>=0"+
      s" AND (batchid=$batchId)"+
      " ORDER BY total_duration_in_minutes desc"+
      " LIMIT 10")


  }
}
