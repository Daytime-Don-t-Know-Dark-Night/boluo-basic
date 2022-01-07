package com.boluo.config;

import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * @Author dingc
 * @Date 2022/1/7 22:31
 */
public class ElasticSearchConfigTest {

	@Test
	public void func1() throws IOException {
		ElasticsearchConfig.getConnection();
	}

	@Test
	public void func2() throws IOException {
		String host = "127.0.0.1";
		String user = "user";
		String pwd = "pwd";
		Map<String, Integer> map = ElasticsearchConfig.getIndicesInfo(host, user, pwd);
		System.out.println(map);
	}


}
