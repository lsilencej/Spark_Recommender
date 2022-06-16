import OfflineRecommender.RATING_COLLECTION
import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @author ：lsilencej
 * @date ：Created in 2022/6/15 22:26
 * @description：${description }
 * @modified By：
 * @version: $version$
 */

object ALSTrainer {
    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://127.0.0.1:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        val ratingRDD = spark.read
            .option("uri", mongoConfig.uri)
            .option("collection", RATING_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[ProductRating]
            .rdd
            .map(
                rating => Rating(rating.userId, rating.productId, rating.score)
            ).cache()

        val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
        val trainingData = splits(0)
        val testingData = splits(1)

        findALSParams(trainingData, testingData)

        spark.stop()
    }

    def findALSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit = {
        val result = for (rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01))
            yield {
                val model = ALS.train(trainData, rank, 10, lambda)
                val rmse = calculateRMSE(model, testData)
                (rank, lambda, rmse)
            }
        println("===========================================================")
        println(result.minBy(_._3))
    }

    def calculateRMSE(model: MatrixFactorizationModel, testData: RDD[Rating]): Double = {
        val userProducts = testData.map(item => (item.user, item.product))
        val predictRating = model.predict(userProducts)

        val observed = testData.map(item => ((item.user, item.product), item.rating))
        val predicted = predictRating.map(item => ((item.user, item.product), item.rating))

        sqrt(
            observed.join(predicted).map {
                case ((userId, productId), (actual, pre)) =>
                    val err = actual - pre
                    err * err
            }.mean()
        )
    }
}
