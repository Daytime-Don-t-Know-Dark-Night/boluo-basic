package com.boluo.config

import com.boluo.config.ESConfig.getConnection
import org.apache.spark.sql.SparkSession

/**
 * @Author dingc
 * @Date 2022/1/10 18:16
 */
object MongoConfig {

    def main(args: Array[String]): Unit = {

        val host = "127.0.0.1"
        val user = "user"
        val pwd = "pwd"
        val database = "boluo"
        val table = "user"
        getConnection(host, user, pwd, database, table)

    }

    def getConnection(host: String, user: String, pwd: String, db: String, collection: String): Unit = {

        // https://www.runoob.com/mongodb/mongodb-tutorial.html
        // mongo标准url   mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]

        val config = Map(
            "mongo.uri" -> s"mongodb://${user}:${pwd}@${host}",
            "mongo.db" -> db, //类似于选择数据库
            "mongo.collection" -> collection //选择数据库中的表
        )

        val spark = SparkSession.builder().master("local[*]").getOrCreate()
        val ds = spark.read
            .option("uri", config("mongo.uri"))
            .option("database", config("mongo.db"))
            .option("collection", config("mongo.collection"))
            .format("mongo")
            .load()

        ds.show(false)

    }

}
