package com.boluo.mysql;

import com.boluo.config.MySQLConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Transaction isolation Test
 *
 * @Author dingc
 * @Date 2022/1/9 20:41
 */
public class TransactionTest {

	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(TransactionTest.class);

	private static final String uri = "jdbc:mysql://localhost:3306/boluo?characterEncoding=UTF-8&serverTimezone=GMT%2B8&rewriteBatchedStatements=true&user=root&password=root";
	private static final String user = "root";
	private static final String pwd = "root";

	@Test
	// https://www.cnblogs.com/huanongying/p/7021555.html
	// https://developer.aliyun.com/article/743691
	// https://www.cnblogs.com/kisun168/p/11320549.html
	public void func1() throws SQLException, ClassNotFoundException {
		Connection conn = MySQLConfig.getConnection(uri);
		init(conn);

		// 脏读测试
		// 1.建立sessionA, 并且设置当前事务为read uncommitted, 开启事务, 查询表中的初始值
		Connection conn1 = MySQLConfig.getConnection(uri);
		conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		Statement statement1 = conn1.createStatement();
		statement1.execute("start transaction");
		ResultSet res1 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 初始化查询结果为: ");
		while (res1.next()) {
			Integer id = res1.getInt("id");
			String name = res1.getString("name");
			String balance = res1.getString("balance");
			System.out.printf("id: '%s', name: '%s', balance: '%s'%n", id, name, balance);
		}

		// 2.建立sessionB, 并且设置当前事务为read uncommitted, 开启事务, 更新表
		Connection conn2 = MySQLConfig.getConnection(uri);
		Statement statement2 = conn2.createStatement();
		statement2.execute("set session transaction isolation level read uncommitted");
		statement2.execute("start transaction");
		int affects = statement2.executeUpdate("update transaction_test set balance = balance - 50 where id = 1");
		Preconditions.checkArgument(affects == 1, "更新数据错误! ");
		// 查看更新之后的数据
		ResultSet res2 = statement2.executeQuery("select * from transaction_test");
		System.out.println("sessionB 更新数据之后查询结果为: ");
		while (res2.next()) {
			Integer id = res2.getInt("id");
			String name = res2.getString("name");
			String balance = res2.getString("balance");
			System.out.printf("id: '%s', name: '%s', balance: '%s'%n", id, name, balance);
		}

		// 3.这时, 虽然sessionB的事务还未提交, 但是sessionA已经可以查询到sessionB更新的数据
		ResultSet res3 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 再次查询结果为: ");
		while (res3.next()) {
			Integer id = res3.getInt("id");
			String name = res3.getString("name");
			String balance = res3.getString("balance");
			System.out.printf("id: '%s', name: '%s', balance: '%s'%n", id, name, balance);
		}

		// 4.如果此时sessionB回滚事务, 一切操作会被撤销, 那么从sessionA查询到的数据就是脏数据
		System.out.println("sessionB 回滚事务");
		statement2.execute("rollback");
		// 查看更新之后的数据
		ResultSet res4 = statement2.executeQuery("select * from transaction_test");
		System.out.println("sessionB 回滚之后的数据查询结果为: ");
		while (res4.next()) {
			Integer id = res4.getInt("id");
			String name = res4.getString("name");
			String balance = res4.getString("balance");
			System.out.printf("id: '%s', name: '%s', balance: '%s'%n", id, name, balance);
		}

		// 5.此时我们更新sessionA中的数据, 显然此时sessionA取到的真正结果也是450, 但是上面的脏读数据400可能被业务层取到使用, 就会出现错误
		affects = statement1.executeUpdate("update transaction_test set balance = balance - 50 where id = 1");
		Preconditions.checkArgument(affects == 1, "更新数据错误! ");
		ResultSet res5 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 更新过后查询结果为: ");
		while (res5.next()) {
			Integer id = res5.getInt("id");
			String name = res5.getString("name");
			String balance = res5.getString("balance");
			System.out.printf("id: '%s', name: '%s', balance: '%s'%n", id, name, balance);
		}

		conn.close();
		conn1.close();
		conn2.close();
	}

	private static void init(Connection conn) throws SQLException {
		// 初始化连接中的测试表
		Statement statement = conn.createStatement();

		// 重新创建测试表
		statement.executeUpdate("truncate table transaction_test");
		statement.executeUpdate("drop table if exists transaction_test");
		statement.executeUpdate("create table transaction_test (`id` int, `name` varchar(60), balance int) DEFAULT CHARSET=utf8mb4");

		// 添加数据
		statement.execute("insert into transaction_test values (1, 'boluo', 450)");
		statement.execute("insert into transaction_test values (2, 'qidai', 1000)");
		statement.execute("insert into transaction_test values (3, 'dingc', 1500)");
	}

}
