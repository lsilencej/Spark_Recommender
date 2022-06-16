import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author ：lsilencej
 * @date ：Created in 2022/6/15 16:09
 * @description：${description }
 * @modified By：
 * @version: $version$
 */

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)
case class MongoConfig(uri: String, db: String)

object StaticRecommender {

    val RATING_COLLECTION = "Rating"
    val RECENTLY_HOT_PRODUCTS_COLLECTION = "RecentlyHotProducts"
    val AVERAGE_RATE_PRODUCTS_COLLECTION = "AverageRateProducts"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[1]",
            "mongo.uri" -> "mongodb://127.0.0.1:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StaticRecommender")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._
        val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        val ratingDF = spark.read
            .option("uri", mongoConfig.uri)
            .option("collection", RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Rating]
            .toDF()

        ratingDF.createOrReplaceTempView("ratingTemp")

        // 统计近期热门商品
        val simpleDateFormat = new SimpleDateFormat("yyyyMM")
        spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
        val ratingByTimeDF = spark.sql("select productId, score, changeDate(timestamp) as time from ratingTemp")
        ratingByTimeDF.createOrReplaceTempView("ratingByTimeTemp")
        val recentlyHotProductsDF = spark.sql("select productId, count(productId) as count, time from ratingByTimeTemp group by time, productId order by time desc, count desc")
        storeDFInMongoDB(recentlyHotProductsDF, RECENTLY_HOT_PRODUCTS_COLLECTION)(mongoConfig)

        // 统计好评商品
        val averageRateProductsDF = spark.sql("select productId, avg(score) as avg from ratingTemp group by productId order by avg desc")
        storeDFInMongoDB(averageRateProductsDF, AVERAGE_RATE_PRODUCTS_COLLECTION)(mongoConfig)

        spark.stop()
    }

    def storeDFInMongoDB(df: DataFrame, collection: String)(mongoConfig: MongoConfig): Unit = {
        df.write
            .option("uri", mongoConfig.uri)
            .option("collection", collection)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()
    }

}
