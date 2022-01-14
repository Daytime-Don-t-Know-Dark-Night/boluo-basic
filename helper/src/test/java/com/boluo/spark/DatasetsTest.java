package com.boluo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.spark.sql.functions.*;

/**
 * @Author dingc
 * @Date 2022/1/14 21:42
 */
public class DatasetsTest {

	@Test
	public void loadTest() {

		String path = "";
		String basePath = "file:///D:/projects/boluo-basic/doc/spark/";

		// 加载txt文件
		path = basePath + "dingc.txt";
		Dataset<Row> ds = Datasets.load(path);
		// ds.show(false);

		// 加载json文件
		path = basePath + "dingc.json";
		ds = Datasets.load(path);
		ds.show(false);

		// 加载csv文件
		path = basePath + "dingc.csv";
		ds = Datasets.load(path);
		Assert.assertEquals(6.0, ds.agg(sum("id")).first().getDouble(0), 0);
		Assert.assertEquals(90.0, ds.agg(sum("age")).first().getDouble(0), 0);

	}

}
