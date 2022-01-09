package com.boluo.mysql;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.delta.util.SetAccumulator;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * MySQL数据库相关操作
 *
 * @Author dingc
 * @Date 2022/1/9 20:28
 */
public class MySQL {

	private static final Logger logger = LoggerFactory.getLogger(MySQL.class);

	/**
	 * @param ds
	 * @param opts
	 * @param where
	 * @throws SQLException
	 * @throws ExecutionException
	 * @throws SparkException
	 * @throws InvocationTargetException
	 */
	public static void insert(Dataset<Row> ds, JdbcOptionsInWrite opts, String where) throws SQLException, ExecutionException, SparkException, InvocationTargetException {

		JdbcDialect dialect = JdbcDialects.get(opts.url());
		long max_allowed_packet = 4 * 1024 * 1024;
		long buffer_pool_size = 128 * 1024 * 1024;

		try (Connection conn = JdbcUtils.createConnectionFactory(opts).apply();
			 Statement statement = conn.createStatement()) {
			if (Objects.isNull(where) || where.equals("1=1")) {
				String sql = String.format("truncate table %s", opts.table());
				statement.executeUpdate(sql);
				System.out.println(sql);
			} else {
				String sql = String.format("delete from %s where %s", opts.table(), where);
				//countBefore -= statement.executeUpdate(sql);
				statement.executeUpdate(sql);
				System.out.println(sql);
			}

			try (ResultSet packetRes = statement.executeQuery("show global variables like 'max_allowed_packet'")) {
				while (packetRes.next()) {
					max_allowed_packet = packetRes.getLong("Value");
				}
			}

			try (ResultSet bufferRes = statement.executeQuery("show global variables like 'innodb_buffer_pool_size'")) {
				while (bufferRes.next()) {
					buffer_pool_size = bufferRes.getLong("Value");
				}
			}
		}

		StructType schema = ds.schema();
		String sql_ = JdbcUtils.getInsertStatement(opts.table(), schema, Option.empty(), true, dialect);

		// sql拼接时不使用''的类型
		List<DataType> specialType = ImmutableList.of(DataTypes.BooleanType, DataTypes.LongType, DataTypes.IntegerType);

		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
		MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage(); //堆内存使用情况
		long totalMemorySize = memoryUsage.getInit(); //初始的总内存
		long maxMemorySize = memoryUsage.getMax(); //最大可用内存
		long usedMemorySize = memoryUsage.getUsed(); //已使用的内存

		long partNum = Math.min(Math.round(0.6 * max_allowed_packet), Math.round(maxMemorySize * 0.06));
		long commitCount = Math.round(0.1 * buffer_pool_size);

		ds.foreachPartition((rows) -> {

			try (Connection conn = JdbcUtils.createConnectionFactory(opts).apply();
				 Statement statement = conn.createStatement()) {
				conn.setAutoCommit(false);

				int numFields = schema.fields().length;
				int partCount = 0, executeCount = 0, sumCount = 0;

				StringBuilder sb = new StringBuilder((int) partNum + 1000);
				String prev = sql_.substring(0, sql_.indexOf("(?"));
				sb.append(prev);

				while (rows.hasNext()) {
					Row row = rows.next();
					StringBuilder group = new StringBuilder("(");
					for (int i = 0; i < numFields; i++) {
						DataType type = schema.apply(i).dataType();

						if (row.isNullAt(i)) {
							// null值处理
							group.append("null,");
						} else if (specialType.contains(type)) {
							// 判断该类型数据是否需要''
							Object tmp = row.getAs(i);
							group.append(String.format("%s,", tmp));
						} else if (type == DataTypes.StringType) {
							// 如果该类型为字符串类型且包含', 则对'进行转义
							String tmp = row.getAs(i);
							group.append(String.format("'%s',", tmp.replaceAll("'", "''")));
						} else {
							Object tmp = row.getAs(i);
							group.append(String.format("'%s',", tmp));
						}
					}
					group.delete(group.length() - 1, group.length());
					group.append("),");
					sb.append(group);

					partCount++;
					if (sb.length() >= partNum) {
						executeCount += sb.length();    // 每执行一次, 累计 + sb.length
						String ex_sql = sb.substring(0, sb.length() - 1);
						statement.executeLargeUpdate(ex_sql);
						sb.setLength(0);
						sb.append(prev);
						partCount = 0;
						logger.info("execute执行次数: {}", sumCount++);
					}

					// 上面每执行一次, 累计 + max_allowed_packet, 累计加到缓冲池的70%, 提交
					if (executeCount >= commitCount) {
						logger.info("commit执行时间: {}", Instant.now());
						conn.commit();
						executeCount = 0;
					}
				}

				// 剩余数量不足partNum的
				if (partCount > 0) {
					String ex_sql = StringUtils.chop(sb.toString());
					statement.executeUpdate(ex_sql);
				}
				conn.commit();
			}

		});
	}

	public static void insert2(Dataset<Row> ds, String uri, JdbcOptionsInWrite opts, int partNum) throws Exception {

		Option<String> max_allowed = opts.parameters().get("max_allowed_factor");
		Option<String> max_memory = opts.parameters().get("max_memory_factor");
		Option<String> max_connected = opts.parameters().get("max_connected_num");

		final double max_allowed_factor = max_allowed.isEmpty() ? 0.3 : Double.parseDouble(max_allowed.get());
		final double max_memory_factor = max_memory.isEmpty() ? 0.02 : Double.parseDouble(max_memory.get());
		final int max_connected_num = max_connected.isEmpty() ? 5 : Integer.parseInt(max_connected.get());

		String url = opts.url().split("\\?")[0];
		String dbName = url.substring(url.lastIndexOf("/") + 1);

		JdbcDialect dialect = JdbcDialects.get(opts.url());
		long max_allowed_packet = 4 * 1024 * 1024;

		try (Connection conn = JdbcUtils.createConnectionFactory(opts).apply(); Statement statement = conn.createStatement()) {
			for (int i = 0; i < partNum; i++) {
				String newTableName = opts.table() + partSuffix(i, partNum);
				try {
					// 截断
					logger.info("截断表: {}", newTableName);
					String truncate_table = String.format("truncate table %s", newTableName);
					statement.executeUpdate(truncate_table);
					// drop
					logger.info("drop表: {}", newTableName);
					String drop_table = String.format("drop table if exists %s", newTableName);
					statement.executeUpdate(drop_table);
				} catch (SQLSyntaxErrorException e) {
					e.printStackTrace();
				}

				// 建表
				scala.collection.immutable.Map<String, String> parameters = opts.parameters();
				parameters = parameters.<String>updated(JDBCOptions.JDBC_TABLE_NAME(), newTableName);
				JdbcOptionsInWrite opt2 = new JdbcOptionsInWrite(parameters);
				logger.info("建表: {}", newTableName);
				tableCol(ds.schema(), opt2);
			}

			try (ResultSet packetRes = statement.executeQuery("show global variables like 'max_allowed_packet'")) {
				while (packetRes.next()) {
					max_allowed_packet = packetRes.getLong("Value");
				}
			}
		}

		StructType schema = ds.schema();
		String sql_ = JdbcUtils.getInsertStatement(opts.table(), schema, Option.empty(), true, dialect);

		// sql拼接时不使用''的类型
		List<DataType> specialType = ImmutableList.of(DataTypes.BooleanType, DataTypes.LongType, DataTypes.IntegerType);

		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
		MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage(); //堆内存使用情况
		long maxMemorySize = memoryUsage.getMax(); //最大可用内存

		SparkContext sparkContext = ds.sparkSession().sparkContext();
		int cores = sparkContext.defaultParallelism();
		int executorNum = sparkContext.getExecutorIds().size() + 1;
		int executorMemory = sparkContext.executorMemory();

		long partLimit = Math.min(
				Math.round(max_allowed_packet * max_allowed_factor),
				Math.round((executorMemory - 300) * 1024 * 1024 * max_memory_factor)
		);
		Preconditions.checkArgument(partLimit > 0, "partLimit计算值<=0");
		int partitionNum = partitionNum(ds.logicalPlan());

		// 重要参数
		System.out.println("重要参数---线程数: " + cores + ", 最大可用内存: " + maxMemorySize + ", executorNum: " + executorNum
				+ ", partLimit: " + partLimit + ", executorMemory: " + executorMemory +
				", 分区数: " + partitionNum);

		SetAccumulator<Integer> collectionAc = new SetAccumulator<>();
		collectionAc.register(SparkSession.active().sparkContext(), Option.apply("setAc"), false);

		Map<Integer, Integer> partitionMap = Maps.newHashMap();
		SparkSession.active().sparkContext().addSparkListener(new SparkListener() {

			public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
				int stageId = stageSubmitted.stageInfo().stageId();
				int numTasks = stageSubmitted.stageInfo().numTasks();
				partitionMap.put(stageId, numTasks);
			}

			public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
				int currStageId = taskEnd.stageId();
				Preconditions.checkArgument(partitionMap.containsKey(currStageId), "当前task中的stageId: " + currStageId + ", map中: " + partitionMap);
				int partitions = partitionMap.get(currStageId);
				synchronized (collectionAc.value()) {
					// 确定该发哪一个
					Set<Integer> currSet = collectionAc.value();
					int index = taskEnd.taskInfo().index();

					logger.info("进入onTaskEnd, taskInfo.index为: {}", index);
					long currIndex = partHash(index, partitionNum, partNum);
					// partitionId % partNum = i的个数
					long num1 = Stream.iterate(0, k -> k + 1).limit(partitions).filter(j -> partHash(j, partitionNum, partNum) == currIndex).count();
					// currSet中的值 % partNum = i的个数
					long num2 = currSet.stream().filter(j -> partHash(j, partitionNum, partNum) == currIndex).count();
					if (num1 == num2) {        // 证明累加器中 % partNum = i的值已经集齐
						String uri_ = uri + partSuffix(currIndex, partNum);
						logger.info("累加器中partNum={}的数据已经导出完成,trigger({})", currIndex, uri_);
						collectionAc.value().removeIf(k -> partHash(k, partitionNum, partNum) == currIndex);
//						try {
//							Youdata.trigger(uri_);
//						} catch (IOException | InterruptedException e) {
//							throw new RuntimeException(e);
//						}
					}
				}
			}
		});

		ds.foreachPartition(rows -> {
			int taskId = TaskContext.getPartitionId();
			long tmpPartNum = partHash(taskId, partitionNum, partNum);

			try (// Connection conn = getConnection(opts, max_connected_num, dbName);
				 Connection conn = JdbcUtils.createConnectionFactory(opts).apply();
				 Statement statement = conn.createStatement()) {
				conn.setAutoCommit(false);
				int numFields = schema.fields().length;
				long executeLength = 0;

				// 替换表名
				String newTableName = opts.table() + partSuffix(tmpPartNum, partNum);
				String sqlPrefix = sql_.substring(0, sql_.indexOf("(?"));
				sqlPrefix = sqlPrefix.replace(sqlPrefix.substring(12, sqlPrefix.indexOf("(")), newTableName);

				StringBuilder sb = new StringBuilder((int) partLimit + 1000);
				sb.append(sqlPrefix);

				while (rows.hasNext()) {
					Row row = rows.next();
					StringBuilder group = new StringBuilder("(");
					for (int i = 0; i < numFields; i++) {
						DataType type = schema.apply(i).dataType();
						if (row.isNullAt(i)) {
							// null值处理
							group.append("null,");
						} else if (specialType.contains(type)) {
							// 判断该类型数据是否需要''
							Object tmp = row.getAs(i);
							group.append(tmp).append(',');
						} else if (type == DataTypes.StringType) {
							// 如果该类型为字符串类型且包含', 则对'进行转义
							String tmp = row.getAs(i);
							group.append("'").append(tmp.replaceAll("'", "''")).append("',");
						} else {
							Object tmp = row.getAs(i);
							group.append("'").append(tmp).append("',");
						}
					}
					group.delete(group.length() - 1, group.length());
					group.append("),");
					sb.append(group);

					if (sb.length() * 2L >= partLimit) {
						// 等待
						for (int insertNum = 999; insertNum > 1; ) {
							insertNum = queryOne(statement, "select count(0) from `information_schema`.`PROCESSLIST` where INFO like '/*YoudataSpark*/%' ");
							if (insertNum > 1) {
								logger.info("正在执行事务数量为: {}, 等待...", insertNum);
								TimeUnit.MILLISECONDS.sleep(Math.round(Math.random() * 5000));
							}
						}

						executeLength += sb.length();    // 每执行一次, 累计 + sb.length
						logger.info("任务ID为: {}, execute过的数据长度: {}, 即将执行的SQL数据长度: {}", taskId, executeLength, sb.length());
						executeUpdateSQL(statement, sb.delete(sb.length() - 1, sb.length()).toString(), 5);
						sb.setLength(0);
						sb.append(sqlPrefix);


						int bufferPageFreeNum = queryOne(statement, "select `FREE_BUFFERS` from information_schema.INNODB_BUFFER_POOL_STATS");
						// 每次execute过后, 查看数据库缓冲池的可用页数量, 如果可用页数量<1000, commit
						if (bufferPageFreeNum < 1000) {
							logger.info("缓冲池剩余空闲页为: {}, 执行commit: {}", bufferPageFreeNum, Instant.now());
							conn.commit();
							executeLength = 0;
						}
					}
				}

				// 剩余还有未被执行的数据
				if (sb.length() > sqlPrefix.length()) {
					String ex_sql = sb.substring(0, sb.length() - 1);
					logger.info("execute过的数据长度: {}, 即将执行的SQL数据长度: {}", executeLength, ex_sql.length());
					executeUpdateSQL(statement, ex_sql, 5);
					sb.setLength(0);
					sb.append(sqlPrefix);
				}
				{
					logger.info("commit执行时间: {}", Instant.now());
					conn.commit();
				}
			}

			collectionAc.add(taskId);    // 0-199
		});
	}

	private static void executeUpdateSQL(Statement statement, String sql, int countDown) throws InterruptedException {
		Preconditions.checkArgument(countDown > 0, "重试次数已用完");
		try {
			statement.executeUpdate("/*YoudataSpark*/" + sql);
		} catch (Exception e) {
			logger.info("执行失败, 重试次数剩余: {}, 错误信息: {}", countDown, e);
			TimeUnit.MILLISECONDS.sleep(Math.round(Math.random() * 5000));
			executeUpdateSQL(statement, sql, countDown - 1);
		}
	}

	private static int queryOne(Statement statement, String sql) throws SQLException {
		ResultSet bufferPageRes = statement.executeQuery("/*YoudataSpark*/" + sql);
		while (bufferPageRes.next()) {
			return bufferPageRes.getInt(1);
		}
		throw new RuntimeException("未查询到结果, sql: " + sql);
	}

	private static Object partSuffix(Object a, int partNum) {
		if (Objects.equals(a, 0) || Objects.equals(a, 0L) || Objects.equals(a, "0")) {
			return "";
		}
		if (partNum >= 8) {
			return String.format("_part%02d", a);
		} else {
			return String.format("_part%s", a);
		}
	}


	private static int partitionNum(LogicalPlan plan) {
		if (plan instanceof Repartition) {
			System.out.println("df.logicalPlan类型为: Repartition");
			Repartition repartition = (Repartition) plan;
			return repartition.numPartitions();
		} else if (plan instanceof Join) {
			System.out.println("df.logicalPlan类型为: Join");
			String partitions = SparkSession.active().conf().get("spark.sql.shuffle.partitions");
			return Integer.parseInt(partitions);
		} else if (plan instanceof Aggregate) {
			System.out.println("df.logicalPlan类型为: Aggregate");
			String partitions = SparkSession.active().conf().get("spark.sql.shuffle.partitions");
			return Integer.parseInt(partitions);
		} else if (plan instanceof LogicalRelation) {
			System.out.println("df.logicalPlan类型为: LogicalRelation");
			String partitions = SparkSession.active().conf().get("spark.sql.shuffle.partitions");
			return Integer.parseInt(partitions);
		} else if (plan instanceof Project) {
			System.out.println("df.logicalPlan类型为: Project");
			return partitionNum(((Project) plan).child());
		} else if (plan instanceof Filter) {
			System.out.println("df.logicalPlan类型为: Filter");
			return partitionNum(((Filter) plan).child());
		} else if (plan instanceof Window) {
			System.out.println("df.logicalPlan类型为: Window");
			return partitionNum(((Window) plan).child());
		} else if (plan instanceof Generate) {
			System.out.println("df.logicalPlan类型为: Generate");
			return partitionNum(((Generate) plan).child());
		} else if (plan instanceof Union) {
			System.out.println("df.logicalPlan类型为: Union");
			scala.collection.immutable.List<LogicalPlan> children = (scala.collection.immutable.List<LogicalPlan>) plan.children();
			int sum = 0;
			for (int i = 0; i < children.size(); i++) {
				sum += partitionNum(children.apply(i));
			}
			return sum;
		} else {
			System.out.println("df.logicalPlan类型为: " + plan);
			return -1;
		}
	}

	private static int partHash(int taskId, int partitionNum, int partNum) {
		if (partitionNum <= 0) {
			return taskId % partNum;
		}
		// 能被整除+0, 否则+1
		taskId += Math.min(1, partitionNum % partNum);
		double part = 1.0 * partitionNum / partNum;
		double shang = taskId / part;
		int ler = (int) Math.floor(shang);
		return Math.min(ler, partNum - 1);
	}

	private static void tableCol(StructType type, JdbcOptionsInWrite opts) throws Exception {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < type.size(); i++) {
			String name = type.apply(i).name();
			String jdbcType = JdbcUtils.getCommonJDBCType(type.apply(i).dataType()).get().databaseTypeDefinition();
			sb.append("`").append(name).append("`").append(" ").append(jdbcType).append(" , ");
		}
		String col_sql = sb.substring(0, sb.length() - 2);

		if (opts.parameters().contains("createTableKeys")) {    // 添加索引
			Outputs.createTable(col_sql, opts, opts.parameters().get("createTableKeys").get(), null);
		} else {
			Outputs.createTable(col_sql, opts, null, null);
		}
	}

	private static Connection getConnection(JdbcOptionsInWrite opts, int defaultConnectedNum, String tableName) throws SQLException {
		// 1.创建连接
		// 2.查询当前的连接数
		// 3.如果当前连接数大于n, 关闭当前连接, 回到第一步
		// 4.使用本连接

		Connection conn = null;
		try {
			conn = JdbcUtils.createConnectionFactory(opts).apply();
			try (Statement statement = conn.createStatement()) {
				// 有效连接数
				ResultSet maxInfo = statement.executeQuery(String.format("select count(0) as `count` from information_schema.processlist where DB = '%s'", tableName));
				int maxConn = 999;
				while (maxInfo.next()) {
					maxConn = maxInfo.getInt("count");
				}

				if (maxConn > defaultConnectedNum) {
					conn.close();
					System.out.println("当前连接数为" + maxConn + ", 等待...");
					TimeUnit.MILLISECONDS.sleep(Math.round(Math.random() * 5000));
					return getConnection(opts, defaultConnectedNum, tableName);
				} else {
					return conn;
				}
			}
		} catch (Exception e) {
			if (Objects.nonNull(conn)) {
				conn.close();
			}
			throw new RuntimeException(e);
		}
	}
}

