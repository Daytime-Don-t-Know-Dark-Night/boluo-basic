package com.boluo.spark;

import com.google.common.base.CharMatcher;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

/**
 * Dataset常用操作的工具类
 *
 * @Author dingc
 * @Date 2022/1/13 21:11
 */
public class Datasets {

	public static Dataset<Row> load(String path) {

		SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
		int index = CharMatcher.anyOf(".").lastIndexIn(path) + 1;
		String fileType = path.substring(index);

		switch (fileType) {
		case "txt": {
			Dataset<String> dsJson = spark.read().textFile(path).map(new MapFunction<String, String>() {
				@Override
				public String call(String str) throws Exception {
					Object[] objs = str.split(" ");
					return RowFactory.create(objs).toString();
				}
			}, Encoders.STRING());
			return spark.read().json(dsJson);

		}
		case "json": {
			return spark.read().json(path);
		}
		case "csv": {
			return spark.read()
					.option("inferSchema", "false")
					.option("header", "true")
					.option("charset", "UTF-8")
					.csv(path);
		}
		}
		throw new IllegalArgumentException("未知的路径或者参数: " + path);
	}
}
