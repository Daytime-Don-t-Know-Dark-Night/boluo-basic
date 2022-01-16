package com.boluo.common;

/**
 * @Author dingc
 * @Date 2022/1/16 17:39
 */
public enum MysqlInfo {

	// 查询事务隔离级别
	TRANSACTION_ISOLATION("select @@transaction_isolation");

	private final String sql;

	MysqlInfo(String sql) {
		this.sql = sql;
	}

	public String value() {
		return this.sql;
	}

	public boolean equals(String value) {
		if (value == null) {
			return false;
		}
		return value.equals(sql);
	}

}
