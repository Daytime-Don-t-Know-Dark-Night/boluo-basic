package com.boluo.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * 针对服务端渲染页面的处理操作
 *
 * @Author dingc
 * @Date 2022/1/11 11:11
 */
public class Documents {

	private static final Logger logger = LoggerFactory.getLogger(Documents.class);
	private static final CloseableHttpClient http;

	static {
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(30);
		cm.setDefaultMaxPerRoute(30);
		http = HttpClients.custom()
				.setConnectionManager(cm)
				.build();
	}

	private static Document getRequest(HttpClientContext httpContext, String url, int countdown) {
		HttpUriRequest req = RequestBuilder.get()
				.setUri(url)
				.setHeader("Referer", "http://boluo.com")
				.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36")
				.build();

		try (CloseableHttpResponse response = http.execute(req, httpContext)) {
			logger.info("{}, 剩余次数{}", req.getURI(), countdown);

			// 出现502,503,504错误时重试
			if (ImmutableList.of(502, 503, 504).contains(response.getStatusLine().getStatusCode()) && countdown > 0) {
				return getRequest(httpContext, url, countdown - 1);
			}
			Preconditions.checkArgument(response.getStatusLine().getStatusCode() == 200, "");
			Thread.sleep(100);
			InputStream is = response.getEntity().getContent();
			return Jsoup.parse(is, "utf-8", "");
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}


}
