import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
 * @author ：lsilencej
 * @date ：Created in 2022/6/15 22:32
 * @description：${description }
 * @modified By：
 * @version: $version$
 */

case class MongoConfig(uri: String, db: String)
case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class Recommendation(productId: Int, score: Double)
case class OfflineUserRecommends(userId: Int, recommends: Seq[Recommendation])
case class ProductRecommends(productId: Int, recommends: Seq[Recommendation])


object OfflineRecommender {

    val RATING_COLLECTION = "Rating"
    val PRODUCT_RECOMMENDS_COLLECTION = "ProductRecommends"
    val OFFLINE_USER_RECOMMENDS_COLLECTION = "OfflineUserRecommends"
    val MAX_RECOMMENDATION = 20

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

        val userRDD = ratingRDD.map(rating => rating.user).distinct()
        val productRDD = ratingRDD.map(rating => rating.product).distinct()

        val (rank, iterations, lambda) = (50, 10, 0.1)
        val model = ALS.train(ratingRDD, rank, iterations, lambda)
        val userProduct = userRDD.cartesian(productRDD)
        val preRating = model.predict(userProduct)

        val userRecommender = preRating.filter(_.rating > 0)
            .map(
                item => (item.user, (item.product, item.rating))
            )
            .groupByKey()
            .map {
                case (userId, recommends) =>
                    OfflineUserRecommends(userId, recommends.toList.sortWith(_._2 > _._2).take(MAX_RECOMMENDATION).map(item => Recommendation(item._1, item._2)))
            }
            .toDF()
        userRecommender.write
            .option("uri", mongoConfig.uri)
            .option("collection", OFFLINE_USER_RECOMMENDS_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        val productFeatures = model.productFeatures.map {
            case (productId, features) => (productId, new DoubleMatrix(features))
        }

        val productRecommender = productFeatures.cartesian(productFeatures)
            .filter {
                case (a, b) => a._1 != b._1
            }
            .map {
                case (a, b) =>
                    val simScore = cousinSim(a._2, b._2)
                    (a._1, (b._1, simScore))
            }
            .filter(_._2._2 > 0.4)
            .groupByKey()
            .map {
                case (productId, recommends) =>
                    ProductRecommends(productId, recommends.toList.sortWith(_._2 > _._2).map(item => Recommendation(item._1, item._2)))
            }
            .toDF()
        productRecommender.write
            .option("uri", mongoConfig.uri)
            .option("collection", PRODUCT_RECOMMENDS_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        spark.stop()
    }

    def cousinSim(a: DoubleMatrix, b: DoubleMatrix): Double = {
        a.dot(b) / (a.norm2() * b.norm2())
    }
}
