package com.boluo.config;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Author dingc
 * @Date 2022/1/16 18:05
 */
public class MySQLConfigTest {

	private static final String uri = "jdbc:mysql://localhost:3306/boluo?characterEncoding=UTF-8&serverTimezone=GMT%2B8&rewriteBatchedStatements=true&user=root&password=root";
	private static final String user = "root";
	private static final String pwd = "root";

	@Test
	public void connectionTest() throws SQLException, ClassNotFoundException {

		// 1. 加载驱动
		// 2. 创建数据库连接
		// 3. 创建statement
		try (Connection conn = MySQLConfig.getConnection(uri, user, pwd); Statement statement = conn.createStatement()) {
			// 4. 执行SQL语句
			ResultSet res = statement.executeQuery("select * from test");

			// 5. 遍历结果集
			while (res.next()) {
				int id = res.getInt("id");
				System.out.println("用户ID为: " + id);
			}

			// 6. 关闭连接
		}

	}

}
