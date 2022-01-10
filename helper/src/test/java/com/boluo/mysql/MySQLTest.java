package com.boluo.mysql;

import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static org.apache.spark.sql.functions.*;

/**
 * MySQL Test
 *
 * @Author dingc
 * @Date 2022/1/9 20:41
 */
public class MySQLTest {

	private static final Logger logger = LoggerFactory.getLogger(MySQLTest.class);

	@Test
	public void insertTest() throws Exception {

		SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
		StructType scheme = new StructType()
				.add("rev", "string")
				.add("项目名称", "string")
				.add("门店名称", "string")
				.add("设备编号", "string")
				.add("金额", "double");

		Row row1 = RowFactory.create("1", "运营体1", "门店1", "设备1", 1.0);
		Row row2 = RowFactory.create("2", "运营体1", "门店2", "设备2", 1.0);
		Row row3 = RowFactory.create("3", "运营体2", "门店1", "设备1", 1.0);

		Dataset<Row> ds = spark.createDataFrame(ImmutableList.of(row1, row2, row3), scheme);
		ds.show(false);

		String test_uri = "jdbc:mysql://127.0.0.1/boluo?characterEncoding=UTF-8&serverTimezone=GMT%2B8&rewriteBatchedStatements=true&user=root&password=root";
		JdbcOptionsInWrite opt = Jdbcs.options(test_uri, "test_table");

		MySQL.insert2(ds, "", opt, 5);
		MySQL.insert2(ds.repartition(200), "", opt, 5);
		MySQL.insert2(ds.repartition(300), "", opt, 5);
		MySQL.insert2(ds.groupBy("设备编号").agg(expr("count(0)")), "", opt, 5);
		MySQL.insert2(ds.where(""), "", opt, 5);
	}
}
