package com.boluo.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.cluster.metadata.AliasMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author dingc
 * @Date 2022/1/7 22:24
 */
public class ElasticsearchConfig {

	private static final ObjectMapper mapper = new ObjectMapper();

	// 连接es, 获取所有索引名称
	public static void getConnection() throws IOException {

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		// 如果有账号密码, 在这里设置账号密码
		// credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("user","password"));

		// 创建rest client对象
		RestClientBuilder builder = RestClient.builder(new HttpHost("127.0.0.1", 9200))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
						return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient client = new RestHighLevelClient(builder);
		System.out.println(client.toString());

		GetRequest getRequest = new GetRequest("dingc", "doc", "100");
		GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
		System.out.println(response.getId());

		try {
			// 获取es连接中所有索引
			GetAliasesRequest request = new GetAliasesRequest();
			GetAliasesResponse getAliasesResponse = client.indices().getAlias(request, RequestOptions.DEFAULT);
			Map<String, Set<AliasMetadata>> map = getAliasesResponse.getAliases();
			Set<String> indices = map.keySet();
			for (String key : indices) {
				System.out.println("索引名称: " + key);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// 连接es, 获取所有索引及全部分片大小
	public static Map<String, Integer> getIndicesInfo(String host, String user, String pwd) throws IOException {

		String auth = user + ":" + pwd;
		byte[] base64 = Base64.getEncoder().encode(auth.getBytes());
		auth = new String(base64);

		// 创建rest client对象
		RestClient lowClient = RestClient
				.builder(HttpHost.create("http://" + host + ":9200"))
				.setDefaultHeaders(new Header[]{
						new BasicHeader("Authorization", "Basic " + auth)
				}).build();

		// es客户端文档: https://www.elastic.co/guide/en/elasticsearch/client/index.html
		// cat_api文档: https://www.elastic.co/guide/en/elasticsearch/reference/7.16/cat.html
		try (InputStream is = lowClient.performRequest(new Request("GET", "/_cat/indices/?v&bytes=mb&format=json")).getEntity().getContent()) {
			JsonNode json = mapper.readTree(is);
			return Streams.stream(json)
					.collect(Collectors.toMap(
							i -> i.at("/index").asText(),
							i -> i.at("/store.size").asInt()
					));
		}
	}

	// spark连接es
	public static void sparkGetConnection() {

	}
}
