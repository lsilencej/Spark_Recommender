import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author ：lsilencej
 * @date ：Created in 2022/6/9 23:56
 * @description：${description }
 * @modified By：
 * @version: $version$
 */

case class Product(productId: Int, name: String, imageUrl: String, categories: String)
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)
case class User(userId: Int, username: String, password: String)

case class MongoConfig(uri: String, db: String)

object DataLoad {
    val PRODUCT_DATA_PATH: String = Thread.currentThread().getContextClassLoader.getResource("products.txt").getPath
    val RATING_DATA_PATH: String = Thread.currentThread().getContextClassLoader.getResource("ratings.txt").getPath
    val USER_DATA_PATH: String = Thread.currentThread().getContextClassLoader.getResource("users.txt").getPath
    val PRODUCT_COLLECTION: String = "Product"
    val RATING_COLLECTION: String = "Rating"
    val USER_COLLECTION = "User"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://127.0.0.1:27017/recommender",
            "mongo.db" -> "recommender"
        )
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
        val productDF = productRDD.map(item => {
            val temp = item.split("\\^");
            Product(temp(0).toInt, temp(1).trim, temp(7).trim, temp(3).trim)
        }).toDF()

        val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
        val ratingDF = ratingRDD.map(item => {
            val temp = item.split(" ")
            Rating(temp(0).toInt, temp(1).toInt, temp(2).toDouble, temp(3).toInt)
        }).toDF()

        val userRDD = spark.sparkContext.textFile(USER_DATA_PATH)
        val userDF = userRDD.map(item => {
            val temp = item.split(" ")
            User(temp(0).toInt, temp(1).trim, temp(2).trim)
        }).toDF()


        val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
        saveInMongoDB(productDF, ratingDF, userDF)(mongoConfig)

        spark.stop()
    }

    def saveInMongoDB(productDF: DataFrame, ratingDF: DataFrame, userDF: DataFrame)(mongoConfig: MongoConfig): Unit = {
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
        val productCollection = mongoClient(mongoConfig.db)(PRODUCT_COLLECTION)
        val ratingCollection = mongoClient(mongoConfig.db)(RATING_COLLECTION)
        val userCollection = mongoClient(mongoConfig.db)(USER_COLLECTION)
        productCollection.dropCollection()
        ratingCollection.dropCollection()
        userCollection.dropCollection()


        productDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", PRODUCT_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        ratingDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", RATING_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()

        userDF.write
            .option("uri", mongoConfig.uri)
            .option("collection", USER_COLLECTION)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()


        productCollection.createIndex(MongoDBObject("productId" -> 1))
        ratingCollection.createIndex(MongoDBObject("productId" -> 1))
        ratingCollection.createIndex(MongoDBObject("userId" -> 1))
        userCollection.createIndex(MongoDBObject("userId" -> 1))

        mongoClient.close()
    }
}
