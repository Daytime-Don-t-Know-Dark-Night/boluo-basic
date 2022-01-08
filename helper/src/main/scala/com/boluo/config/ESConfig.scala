package com.boluo.config

import org.apache.spark.sql.SparkSession

/**
 * @Author dingc
 * @Date 2022/1/8 11:00
 */
object ESConfig {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local[*]").getOrCreate()

        val host = "127.0.0.1"
        val user = "user"
        val pwd = "pwd"
        getConnection(spark, host, user, pwd)
    }

    // spark连接es
    def getConnection(spark: SparkSession, host: String, user: String, pwd: String): Unit = {

        val options = Map(
            "es.nodes.wan.only" -> "true",
            "es.nodes" -> host,
            "es.port" -> "9200",
            "es.scroll.size" -> "1000",
            "es.net.http.auth.user" -> user,
            "es.net.http.auth.pass" -> pwd
        )

        val ds = spark.read
            .format("es")
            .options(options)
            .load("dingc")
        ds.show(false)
    }

}
