package com.boluo.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author dingc
 * @Date 2022/1/16 17:55
 */
public class MySQLConfig {

	public static Connection getConnection(String uri, String user, String pwd) throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.cj.jdbc.Driver");
		return DriverManager.getConnection(uri, user, pwd);
	}

}
