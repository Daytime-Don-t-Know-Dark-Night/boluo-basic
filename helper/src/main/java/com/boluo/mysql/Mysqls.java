package com.boluo.mysql;

import com.boluo.common.MysqlInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL工具类
 *
 * @Author dingc
 * @Date 2022/1/16 17:30
 */
public class Mysqls {

	private static final ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = LoggerFactory.getLogger(Mysqls.class);

	// 查看MySQL中重要参数
	public static JsonNode check(Connection conn, MysqlInfo type) throws SQLException {
		// 此处不使用switch是因为case后无法使用表达式
		if (type.equals(MysqlInfo.TRANSACTION_ISOLATION)) {
			return getTransactionIsolation(conn, type.value());
		}
		throw new IllegalArgumentException("未知的参数: " + type);
	}

	// 查看数据库的事务隔离级别
	private static JsonNode getTransactionIsolation(Connection conn, String sql) throws SQLException {
		ObjectNode obj = mapper.createObjectNode();
		Statement statement = conn.createStatement();
		ResultSet res = statement.executeQuery(sql);
		while (res.next()) {
			obj.put("transactionIsolation", res.getString("@@transaction_isolation"));
		}
		System.out.println("数据库事务隔离级别为: " + obj);
		return obj;
	}

}
