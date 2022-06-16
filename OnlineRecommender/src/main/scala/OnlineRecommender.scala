/**
 * @author ：lsilencej
 * @date ：Created in 2022/6/16 17:17
 * @description：${description }
 * @modified By：
 * @version: $version$
 */
object OnlineRecommender {

    val RATING_COLLECTION = "Rating"
    val PRODUCT_RECOMMENDS_COLLECTION = "ProductRecommends"
    val ONLINE_USER_RECOMMENDS_COLLECTION = "OnlineUserRecommends"

    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://127.0.0.1:27017/recommender",
            "mongo.db" -> "recommender",
            "kafka.topic" -> "recommender"
        )
    }
}
