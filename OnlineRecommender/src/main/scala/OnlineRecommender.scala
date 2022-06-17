import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author ：lsilencej
 * @date ：Created in 2022/6/16 17:17
 * @description：${description }
 * @modified By：
 * @version: $version$
 */

case class MongoConfig(uri: String, db: String)

case class Recommendation(productId: Int, score: Double)
case class ProductRecommends(productId: Int, recommends: Seq[Recommendation])

object ConnRedisMongodb extends Serializable {
    lazy val jedis = new Jedis("127.0.0.1")
    lazy val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://127.0.0.1:27017/recommender"))
}

object OnlineRecommender {

    val RATING_COLLECTION = "Rating"
    val PRODUCT_RECOMMENDS_COLLECTION = "ProductRecommends"
    val ONLINE_USER_RECOMMENDS_COLLECTION = "OnlineUserRecommends"
    val MAX_USER_RATINGS_NUM = 20
    val MAX_SIMILAR_PRODUCTS_NUM = 20

    def main(args: Array[String]): Unit = {

        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://127.0.0.1:27017/recommender",
            "mongo.db" -> "recommender",
            "kafka.topic" -> "recommender"
        )

        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        val sc = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(2))

        import spark.implicits._

        val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

        val kafkaParam = Map(
            "bootstrap.servers" -> "127.0.0.1:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "recommender",
            "auto.offset.reset" -> "latest"
        )

        val similarProductMatrix = spark.read
            .option("uri", mongoConfig.uri)
            .option("collection", PRODUCT_RECOMMENDS_COLLECTION)
            .format("com.mongodb.spark.sql")
            .load()
            .as[ProductRecommends]
            .rdd
            .map(
                item => (item.productId, item.recommends.map(i => (i.productId, i.score)).toMap)
            )
            .collectAsMap()

        val similarProductMatrixBroadcast = sc.broadcast(similarProductMatrix)

        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
        )

        // userId|productId|score|timestamp
        val ratingStream = kafkaStream.map {
            msg =>
                var temp = msg.value().split("\\|")
                (temp(0).toInt, temp(1).toInt, temp(2).toDouble, temp(3).toInt)
        }

        ratingStream.foreachRDD {
            items => items.foreach {
                    case (userId, productId, score, timestamp) =>
                        println("==================================================")
                        println(userId + "|" + productId + "|" + score + "|" + timestamp)

                        // Array[(productId, score)]
                        val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATINGS_NUM, userId, ConnRedisMongodb.jedis)

                        // Array[productId]
                        val filterSimilarProducts = getFilterSimilarProducts(MAX_SIMILAR_PRODUCTS_NUM, userId, productId, similarProductMatrixBroadcast.value)(mongoConfig)

                        // Array[(productId, score)]
                        val recommendLevelList = getRecommendLevels(userRecentlyRatings, filterSimilarProducts, similarProductMatrixBroadcast.value)

                        // save to mongodb
                        saveToMongoDB(userId, recommendLevelList)(mongoConfig)

            }
        }

        ssc.start()

        println("streaming start")

        ssc.awaitTermination()
    }

    import scala.collection.JavaConversions._

    // key: userId
    // value: productId:score
    def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
        jedis.lrange("userId:" + userId.toString, 0, num)
            .map {
                item =>
                    val temp = item.split("\\:")
                    (temp(0).trim.toInt, temp(1).trim.toDouble)
            }
            .toArray
    }

    def getFilterSimilarProducts(num: Int, userId: Int, productId: Int, similarProducts: scala.collection.Map[Int, scala.collection.Map[Int, Double]])(mongoConfig: MongoConfig): Array[Int] = {
        val allSimilarProducts = similarProducts(productId).toArray
        val ratingCollection = ConnRedisMongodb.mongoClient(mongoConfig.db)(RATING_COLLECTION)
        val haveRatingProducts = ratingCollection.find(MongoDBObject("userId" -> userId))
            .toArray
            .map(
                item => item.get("productId").toString.toInt
            )
        allSimilarProducts.filter(i => !haveRatingProducts.contains(i._1))
            .sortWith(_._2 > _._2)
            .take(num)
            .map(item => item._1)
    }

    def getProductsSimilarScore(product1: Int, product2: Int, similarProducts: scala.collection.Map[Int, scala.collection.Map[Int, Double]]): Double = {
        similarProducts.get(product1) match {
            case Some(x) => x.get(product2) match {
                case Some(score) => score
                case None => 0.0
            }
            case None => 0.0
        }
    }

    def lg(i: Int): Double = {
        math.log(i) / math.log(10)
    }

    def getRecommendLevels(userRecentlyRatings: Array[(Int, Double)], filterSimilarProducts:Array[Int], similarProducts: scala.collection.Map[Int, scala.collection.Map[Int, Double]]): Array[(Int, Double)] = {
        // (productId, score)
        val productScores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

        // productId -> count
        val inCountMap = scala.collection.mutable.HashMap[Int, Int]()
        val reCountMap = scala.collection.mutable.HashMap[Int, Int]()

        for (filterSimilarProduct <- filterSimilarProducts; userRecentlyRating <- userRecentlyRatings) {
            val similarScore = getProductsSimilarScore(filterSimilarProduct, userRecentlyRating._1, similarProducts)
            if (similarScore > 0.4) {
                productScores += ((filterSimilarProduct, similarScore * userRecentlyRating._2))
                if (userRecentlyRating._2 > 3) {
                    inCountMap(filterSimilarProduct) = inCountMap.getOrDefault(filterSimilarProduct, 0) + 1
                } else {
                    reCountMap(filterSimilarProduct) = reCountMap.getOrDefault(filterSimilarProduct, 0) + 1
                }
            }
        }

        productScores.groupBy(_._1)
            .map {
            case (productId, scores) =>
                (productId, scores.map(_._2).sum / scores.length + lg(inCountMap.getOrDefault(productId, 1)) - lg(reCountMap.getOrDefault(productId, 1)))
            }
            .toArray
            .sortWith(_._2 > _._2)
    }

    def saveToMongoDB(userId: Int, recommendLevelList: Array[(Int, Double)])(mongoConfig: MongoConfig): Unit = {
        val onlineUserRecommendsCollection = ConnRedisMongodb.mongoClient(mongoConfig.db)(ONLINE_USER_RECOMMENDS_COLLECTION)
        onlineUserRecommendsCollection.findAndRemove(MongoDBObject("userId" -> userId))
        onlineUserRecommendsCollection.insert(MongoDBObject("userId" -> userId, "recommends" -> recommendLevelList.map(item => MongoDBObject("productId" -> item._1, "score" -> item._2))))
    }

}
