package com.boluo.report;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * 监控报告工具类
 *
 * @Author dingc
 * @Date 2022/2/11 18:07
 */
public class Reports {

	/**
	 * 切分消息, 从换行处切分
	 *
	 * @param message 消息体
	 * @param maxSize 切分块最大阈值
	 * @return
	 */
	public static Iterable<String> splitContent(String message, int maxSize) {
		if (message.length() < maxSize) {
			return ImmutableList.of(message);
		}
		// 这里找maxSize上一个换行位置,切分成2份
		int idx = message.lastIndexOf('\n', maxSize);
		Preconditions.checkArgument(idx != -1, "未从消息体中向前找到换行符: " + message + ", 阈值: " + maxSize);

		Iterable<String> a = ImmutableList.of(message.substring(0, idx));
		Iterable<String> b = splitContent(message.substring(idx + 1), maxSize);
		return Iterables.concat(a, b);
	}

	public static Iterable<Iterable<String>> splitContentAll(String message, int maxSize) {
		return ImmutableList.of(
				splitContent(message, maxSize)
		);
	}

}
