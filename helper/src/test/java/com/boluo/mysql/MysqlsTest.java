package com.boluo.mysql;

import com.boluo.common.MysqlInfo;
import com.boluo.config.MySQLConfig;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Author dingc
 * @Date 2022/1/16 18:45
 */
public class MysqlsTest {

	private static final String uri = "jdbc:mysql://localhost:3306/boluo?characterEncoding=UTF-8&serverTimezone=GMT%2B8&rewriteBatchedStatements=true&user=root&password=root";
	private static final String user = "root";
	private static final String pwd = "root";

	@Test
	public void checkTest() throws SQLException, ClassNotFoundException {
		try (Connection conn = MySQLConfig.getConnection(uri, user, pwd)) {
			// 查看事务隔离级别
			JsonNode jn = Mysqls.check(conn, MysqlInfo.TRANSACTION_ISOLATION);
			System.out.println(jn);
		}

	}
}
