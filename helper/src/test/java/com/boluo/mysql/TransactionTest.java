package com.boluo.mysql;

import com.boluo.config.MySQLConfig;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Transaction isolation Test
 *
 * @Author dingc
 * @Date 2022/1/9 20:41
 */
public class TransactionTest {

	private static final String uri = "jdbc:mysql://localhost:3306/boluo?characterEncoding=UTF-8&serverTimezone=GMT%2B8&rewriteBatchedStatements=true&user=root&password=root";
	private static final String user = "root";
	private static final String pwd = "root";

	@Test
	// https://www.cnblogs.com/huanongying/p/7021555.html
	// https://developer.aliyun.com/article/743691
	// https://www.cnblogs.com/kisun168/p/11320549.html
	public void func1() throws SQLException, ClassNotFoundException {
		// 脏读测试
		Connection conn = MySQLConfig.getConnection(uri);
		Statement statement = conn.createStatement();


	}

	private static void init(Connection conn) throws SQLException {
		// 初始化连接中的测试表
		conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		Statement statement = conn.createStatement();

		// 重新创建测试表
		statement.executeUpdate("truncate table test");
		statement.executeUpdate("drop table if exists test");
		statement.executeUpdate("create table test (`id` int, `name` varchar(60), balance int) DEFAULT CHARSET=utf8mb4");
	}

}
