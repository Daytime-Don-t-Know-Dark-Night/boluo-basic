package com.boluo.mysql;

import com.boluo.config.MySQLConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Transaction isolation Test
 *
 * @Author dingc
 * @Date 2022/1/9 20:41
 */
public class TransactionTest {

	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(TransactionTest.class);

	private static final String uri = "jdbc:mysql://local.landeli.com/test_boluo?serverTimezone=GMT%2B8&rewriteBatchedStatements=true&autoReconnect=true&user=root&password=Xlpro2019";

	@Test
	// https://www.cnblogs.com/huanongying/p/7021555.html
	// https://developer.aliyun.com/article/743691
	// https://www.cnblogs.com/kisun168/p/11320549.html
	public void func1() throws SQLException, ClassNotFoundException {
		Connection conn = MySQLConfig.getConnection(uri);
		init(conn);

		// 脏读测试
		// 1.建立sessionA, 并且设置当前事务为read uncommitted(读未提交), 开启事务, 查询表中的初始值
		Connection conn1 = MySQLConfig.getConnection(uri);
		conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
		Statement statement1 = conn1.createStatement();
		statement1.execute("start transaction");
		ResultSet res1 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 初始化查询结果为: ");
		show(res1);

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
		show(res2);

		// 3.这时, 虽然sessionB的事务还未提交, 但是sessionA已经可以查询到sessionB更新的数据
		ResultSet res3 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 再次查询结果为: ");
		show(res3);

		// 4.如果此时sessionB回滚事务, 一切操作会被撤销, 那么从sessionA查询到的数据就是脏数据
		System.out.println("sessionB 回滚事务");
		statement2.execute("rollback");
		// 查看更新之后的数据
		ResultSet res4 = statement2.executeQuery("select * from transaction_test");
		System.out.println("sessionB 回滚之后的数据查询结果为: ");
		show(res4);

		// 5.此时我们更新sessionA中的数据, 显然此时sessionA取到的真正结果也是450, 但是上面的脏读数据400可能被业务层取到使用, 就会出现错误
		affects = statement1.executeUpdate("update transaction_test set balance = balance - 50 where id = 1");
		Preconditions.checkArgument(affects == 1, "更新数据错误! ");
		ResultSet res5 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 更新过后查询结果为: ");
		show(res5);

		conn.close();
		conn1.close();
		conn2.close();
	}

	@Test
	public void func2() throws SQLException, ClassNotFoundException {
		Connection conn = MySQLConfig.getConnection(uri);
		init(conn);

		// 不可重复读测试
		// 1.建立sessionA, 并且设置当前事务模式为read committed(读已提交), 开启事务
		Connection conn1 = MySQLConfig.getConnection(uri);
		conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		Statement statement1 = conn1.createStatement();
		statement1.execute("start transaction");
		ResultSet res1 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 初始化查询结果为: ");
		show(res1);

		// 2.在sessionA的事务未提交之前, 建立sessionB, 更新表
		Connection conn2 = MySQLConfig.getConnection(uri);
		Statement statement2 = conn2.createStatement();
		statement2.execute("set session transaction isolation level read committed");
		statement2.execute("start transaction");
		statement2.executeUpdate("update transaction_test set balance = balance - 50 where id = 1");
		ResultSet res2 = statement2.executeQuery("select * from transaction_test");
		System.out.println("sessionB 更新数据之后查询结果为: ");
		show(res2);

		// 3.这时候, sessionB的事务还未提交, sessionA已经不能查询到sessionB更新的数据, 解决了脏读问题
		ResultSet res3 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 再次查询结果已经解决脏读问题: ");
		show(res3);

		// 4.提交sessionB的事务
		System.out.println("sessionB 提交事务");
		statement2.execute("commit");

		// 5.sessionA再次执行查询, 会产生与sessionB提交前不一样的结果, 产生了不可重复读问题
		ResultSet res5 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 再次查询结果, 产生了不可重复读问题");
		show(res5);

		conn.close();
		conn1.close();
		conn2.close();
	}

	@Test
	public void func3() throws SQLException, ClassNotFoundException {
		Connection conn = MySQLConfig.getConnection(uri);
		init(conn);

		// 幻读测试
		// 1.建立sessionA, 并且设置当前事务模式为repeatable read(可重复读), 开启事务
		Connection conn1 = MySQLConfig.getConnection(uri);
		conn1.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		Statement statement1 = conn1.createStatement();
		statement1.execute("start transaction");
		ResultSet res1 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 初始化查询结果为: ");
		show(res1);

		// 2.建立sessionB, 并且插入一条新数据
		Connection conn2 = MySQLConfig.getConnection(uri);
		conn2.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		Statement statement2 = conn2.createStatement();
		statement2.execute("start transaction");
		int num = statement2.executeUpdate("insert into transaction_test values (4, 'ceshi', 2000)");
		Preconditions.checkArgument(num == 1, "插入数据失败! ");
		ResultSet res2 = statement2.executeQuery("select * from transaction_test");
		System.out.println("sessionB 插入新数据之后查询结果为: ");
		show(res2);

		// 3.在sessionB提交事务之前, 再次查询sessionA的数据, mvvc解决了读的幻读, 使用读未提交隔离机制可复现幻读结果
		ResultSet res3 = statement1.executeQuery("select * from transaction_test");
		System.out.println("sessionA 再次查询结果为: ");
		show(res3);
	}

	private static void init(Connection conn) throws SQLException {
		// 初始化连接中的测试表
		Statement statement = conn.createStatement();

		try {
			// 删除测试表
			statement.executeUpdate("truncate table transaction_test");
			statement.executeUpdate("drop table if exists transaction_test");
		} catch (SQLSyntaxErrorException e) {
			e.printStackTrace();
		}

		// 添加数据
		statement.executeUpdate("create table transaction_test (`id` int, `name` varchar(60), balance int) DEFAULT CHARSET=utf8mb4");
		statement.execute("insert into transaction_test values (1, 'boluo', 450)");
		statement.execute("insert into transaction_test values (2, 'qidai', 1000)");
		statement.execute("insert into transaction_test values (3, 'dingc', 1500)");
	}

	private static void show(ResultSet res) throws SQLException {
		while (res.next()) {
			Integer id = res.getInt("id");
			String name = res.getString("name");
			String balance = res.getString("balance");
			System.out.printf("id: '%s', name: '%s', balance: '%s'%n", id, name, balance);
		}
	}

}
