package com.boluo.mysql;

import com.google.common.base.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import io.delta.tables.DeltaTable;
import org.apache.spark.SparkException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @Author dingc
 * @Date 2022/1/9 20:23
 */
public class Outputs {

	private static final Logger logger = LoggerFactory.getLogger(Outputs.class);

	public static String[] metaInternal(JDBCOptions opt) throws SQLException {
		try (Connection conn = JdbcUtils.createConnectionFactory(opt).apply();
			 Statement statement = conn.createStatement()) {
			ResultSet last = statement.executeQuery(String.format("show create table `%s`", opt.tableOrQuery()));
			Preconditions.checkArgument(last.first());
			String ddl = last.getString("create table");
			Matcher matcher = Pattern.compile("(\n|.)*COMMENT='([^']+)'.*").matcher(ddl);
			if (!matcher.matches()) {
				return new String[0];
			}
			String comment = matcher.group(2);
			return comment.split("(\\\\r)?(\\\\n)");
		} catch (SQLSyntaxErrorException ex) {
			if (ex.getSQLState().equals("42S02")) {
				return new String[0];
			}
			throw ex;
		}
	}

	public static void metaInternal(JdbcOptionsInWrite opts, String... comment) throws SQLException {
		logger.info("meta {}", String.join(",", comment));
		Jdbcs.execute(opts.url(), String.format("alter table `%s` comment '%s'", opts.table(), String.join("\n", comment)));
	}

	public static Map<String, String> meta(JDBCOptions opt) throws SQLException {
		Pattern pattern = Pattern.compile("([^=]+)=(.*)");
		return Arrays.stream(metaInternal(opt))
				.map(i -> {
					Matcher matcher = pattern.matcher(i);
					Preconditions.checkArgument(matcher.matches());
					return matcher;
				})
				.filter(i -> !i.group(2).equals("null"))
				.collect(Collectors.toMap(i -> i.group(1), i -> i.group(2)));
	}

	public static void meta(JdbcOptionsInWrite opts, Object last) throws SQLException {
		Outputs.metaInternal(opts, "last=" + last, "update=" + Instant.now());
	}

	/******************************************************************************************************************/
	/**
	 * 基于hdfs的元数据存储
	 */
	public static Map<String, String> meta(String uri) {
		String dir = SparkSession.active().conf().get("meta.dir", "_meta");
//		Class<?> pathFilter = SparkSession.active().sparkContext().hadoopConfiguration()
//				.getClass("mapreduce.input.pathFilter.class", null);
//		if (Objects.nonNull(pathFilter) && pathFilter.getName().contains("org.apache.hudi")) {
//			dir = ".hoodie/" + dir;
//		}
		String uriMeta = CharMatcher.anyOf("/").trimTrailingFrom(uri) + "/" + dir;
		Map<String, String> res = Maps.newConcurrentMap();
		try {
			SparkSession.active().read().json(uriMeta)
					.collectAsList()
					.forEach(row -> res.put(row.getString(0), row.getString(1)));
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
		return res;
	}

//	未知bug, 记得修复
//	public static void meta(String uri, Map<String, String> kv) {
//		String dir = SparkSession.active().conf().get("meta.dir", "_meta");
//		String uriMeta = CharMatcher.anyOf("/").trimTrailingFrom(uri) + "/" + dir;
//		List<Tuple2<String, String>> collect = kv.entrySet().stream()
//				.map(i -> Tuple2.apply(i.getKey(), i.getValue()))
//				.collect(Collectors.toList());
//		Dataset<Tuple2<String, String>> ds = SparkSession.active().createDataset(collect,
//				Encoders.tuple(Encoders.STRING(), Encoders.STRING())
//		);
//		ds.selectExpr("_1 k", "_2 v")
//				.coalesce(1)
//				.write()
//				.mode(SaveMode.Overwrite)
//				.json(uriMeta);
//	}

	/******************************************************************************************************************/

	@SuppressWarnings("unchecked")
	public static <T> T maxValue(JDBCOptions opts, String col) throws SQLException {
		try (Connection conn = JdbcUtils.createConnectionFactory(opts).apply();
			 Statement statement = conn.createStatement()) {
			try (ResultSet rs = statement.executeQuery(String.format("select max(`%s`) from `%s`", col, opts.tableOrQuery()))) {
				if (!rs.first()) {
					return null;
				}
				return (T) rs.getObject(1);
			}
		}
	}

	/******************************************************************************************************************/

	public static void createTable(Dataset<Row> rows, JdbcOptionsInWrite options, String keys) throws Exception {
		createTable(rows, options, keys, null);
	}

	public static void createTable(Dataset<Row> rows, JdbcOptionsInWrite options, Jdbcs.ConnectConsumer consumer) throws Exception {
		createTable(rows, options, null, consumer);
	}

	public static void createTable(Dataset<Row> rows, JdbcOptionsInWrite options, String keys, Jdbcs.ConnectConsumer consumer) throws Exception {
		String strSchema = JdbcUtils.schemaString(rows, options.url(), Option.empty());
		createTable(strSchema, options, keys, consumer);
	}

	public static void createTable(String strSchema, JdbcOptionsInWrite options, String keys, Jdbcs.ConnectConsumer consumer) throws Exception {
		JdbcDialect dialect = JdbcDialects.get(options.url());

		String keyStr = "";
		if (!Strings.isNullOrEmpty(keys)) {
			Pattern patternKey = Pattern.compile("(primary\\s+)?key\\s*\\([^)]*\\)", Pattern.CASE_INSENSITIVE);
			Matcher matcher = patternKey.matcher(keys);
			if (matcher.find()) {
				keyStr = "," + keys;
			} else {
				keyStr = ",PRIMARY KEY("
						+ Streams.stream(
						Splitter.on(",").trimResults().omitEmptyStrings()
								.split(keys))
						.map(i -> CharMatcher.anyOf("`").trimFrom(i))
						.map(i -> String.format("`%s`", i))
						.collect(Collectors.joining(","))
						+ ")";
			}
		}

		// 自定义字段类型
		if (!options.createTableColumnTypes().isEmpty()) {
			Pattern pat = Pattern.compile("\\s*`?\"?(.+?)`?\"?\\s+(.+)");
			Map<String, String> collect1 = Arrays.stream(strSchema.split(" , "))
					.collect(Collectors.toMap(i -> {
						Matcher matcher = pat.matcher(i);
						Preconditions.checkArgument(matcher.matches(), i);
						return dialect.quoteIdentifier(matcher.group(1));
					}, i -> i));
			Arrays.stream(options.createTableColumnTypes().get().split(","))
					.forEach(i -> {
						Matcher matcher = pat.matcher(i);
						Preconditions.checkArgument(matcher.matches());
						String k = dialect.quoteIdentifier(matcher.group(1));
						String t = matcher.group(2);
						Preconditions.checkArgument(collect1.containsKey(k), String.format("%s not in %s", k, collect1));
						collect1.put(k, k + " " + t);
					});
			strSchema = Joiner.on(",").join(collect1.values());
		}

		String table = options.table();
		String createTableOptions = options.createTableOptions();
		String sql;
		if (options.driverClass().equals("org.postgresql.Driver")) {
			sql = String.format("CREATE TABLE %s (%s%s) %s", table, strSchema, keyStr, createTableOptions);
		} else {
			sql = String.format("CREATE TABLE %s (%s%s) DEFAULT CHARSET=utf8mb4 %s", table, strSchema, keyStr, createTableOptions);
		}
		try (Connection conn = JdbcUtils.createConnectionFactory(options).apply();
			 Statement statement = conn.createStatement()
		) {
			boolean exists = JdbcUtils.tableExists(conn, options);
			if (!exists) {
				// statement.setQueryTimeout(options.queryTimeout());
				logger.info("即将执行SQL语句: {}", sql);
				statement.executeUpdate(sql);
			}
			if (Objects.nonNull(consumer)) {
				consumer.accept(statement, exists);
			}
		}
	}

	/******************************************************************************************************************/

	public static void replace(Dataset<Row> ds, JdbcOptionsInWrite opts, String where) throws SQLException, ExecutionException, SparkException, InvocationTargetException {
		if (ds != null) {
			/*ds.foreachPartition((it) -> {
				new IllegalArgumentException("");
			});*/
			//throw new UndeclaredThrowableException(new IllegalArgumentException(""));
		}

		JdbcDialect dialect = JdbcDialects.get(opts.url());
		long countBefore;
		try (Connection conn = JdbcUtils.createConnectionFactory(opts).apply();
			 Statement statement = conn.createStatement()) {
			/*ResultSet rs = statement.executeQuery(String.format("select count(0) from %s", dialect.quoteIdentifier(opts.table())));
			Preconditions.checkArgument(rs.next());
			countBefore = rs.getLong(1);*/

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
		}

//		ds.write().format("jdbc")
//				.mode(SaveMode.Append)
//				.options(opts.parameters())
//				.save();
		StructType schema = ds.schema();
		String sql_ = JdbcUtils.getInsertStatement(opts.table(), schema, Option.empty(), true, dialect);
		String sql;
		if (opts.driverClass().equals("org.postgresql.Driver")) {
			sql = sql_; /*+ " on conflict do update " +
					Arrays.stream(schema.fieldNames())
							.map(i -> String.format("set %s=excluded.%s", dialect.quoteIdentifier(i), dialect.quoteIdentifier(i)))
							.collect(Collectors.joining(","));*/
		} else {
			sql = sql_ + " on duplicate key update " +
					Arrays.stream(schema.fieldNames())
							.map(i -> String.format("%s=values(%s)", dialect.quoteIdentifier(i), dialect.quoteIdentifier(i)))
							.collect(Collectors.joining(","));
		}
//		ds.cache();
//		long countDelta = ds.count();
		ds.foreachPartition((rows) -> {
			Function0<Connection> connectionFactory = JdbcUtils.createConnectionFactory(opts);
			JdbcUtils.savePartition(connectionFactory,
					opts.table(),
					JavaConverters.asScalaIteratorConverter(rows).asScala(),
					schema, sql, 5000,
					dialect, Connection.TRANSACTION_NONE, opts);
		});
//		ds.unpersist();

		/*long countAfter;
		try (Connection conn = JdbcUtils.createConnectionFactory(opts).apply();
			 Statement statement = conn.createStatement()) {
			ResultSet rs = statement.executeQuery(String.format("select count(0) from `%s`", opts.table()));
			Preconditions.checkArgument(rs.next());
			countAfter = rs.getLong(1);
		}*/
		//logger.info("before={},after={},rows={}", countBefore, countAfter, -1);
		//if (countBefore + countDelta != countAfter) {
//		String msg = String.format("%d+%d!=%d", countBefore, countDelta, countAfter);
//		throw new ExecutionException(new IllegalStateException(msg));
		//}
	}

	public static void replace(Dataset<Row> ds, String uri, Column condition, String... hash) {
		DeltaTable delta = DeltaTable.isDeltaTable(uri)
				? DeltaTable.forPath(SparkSession.active(), uri)
				: null;
		if (Objects.nonNull(delta) && delta.toDF().schema().isEmpty()) {
			delta = null;
		}
		if (Objects.isNull(delta)) {
			// 第一次保存
			ds.write().format("delta")
					.partitionBy(hash)
					.mode(SaveMode.Overwrite)
					.save(uri);
		} else {
//			delta.vacuum();
			delta.as("t")
					.merge(ds.as("s"), condition)
					.whenMatched().updateAll()
					.whenNotMatched().insertAll()
					.execute();
		}
	}

}

