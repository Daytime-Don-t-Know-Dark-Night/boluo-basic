package com.boluo.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * @Author dingc
 * @Date 2022/1/14 21:42
 */
public class DatasetsTest {

	private static final ObjectMapper mapper = new ObjectMapper();

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

	@Test
	public void list2RDDTest() {

		ObjectNode obj1 = mapper.createObjectNode();
		obj1.put("id", "1");
		obj1.put("name", "dingc");
		obj1.put("age", "20");

		ObjectNode obj2 = mapper.createObjectNode();
		obj2.put("id", "2");
		obj2.put("name", "boluo");
		obj2.put("age", "30");

		ObjectNode obj3 = mapper.createObjectNode();
		obj3.put("id", "3");
		obj3.put("name", "qidai");
		obj3.put("age", "40");

		List<ObjectNode> list = Lists.newArrayList(obj1, obj2, obj3);
		Dataset<Row> ds = Datasets.list2Ds(list);
		ds.show(false);
	}

}
