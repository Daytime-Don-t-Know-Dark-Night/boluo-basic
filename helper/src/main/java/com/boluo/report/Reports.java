package com.boluo.report;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * 监控报告工具类
 *
 * @Author dingc
 * @Date 2022/2/11 18:07
 */
public class Reports {

	public static Iterable<String> split(String message, int size) {
		if (message.length() <= size) {
			return ImmutableList.of(message);
		}
		// 从size处切割为两部分, 返回
		return ImmutableList.of(message.substring(0, size), message.substring(size));
	}

	/**
	 * 求消息体在不超过阈值的前提下, 所有的分割情况
	 *
	 * @param message 消息体
	 * @param size    阈值
	 * @return
	 */
	@SuppressWarnings("all")
	public static Iterable<Iterable<String>> splitAll(String message, int size) {

		// abc 切割所有情况
		Set<Iterable<String>> res = Sets.newTreeSet(new Comparator<Iterable<String>>() {
			@Override
			public int compare(Iterable<String> a, Iterable<String> b) {
				return Iterables.elementsEqual(a, b) ? 0 : 1;
			}
		});

		// 从msg最大值开始往后切割 4,3,2,1...
		for (int i = message.length(); i > 0; i--) {
			// 将消息从i处切割
			Iterable<String> contents = split(message, i);
			// 如果消息被切成了两部分
			if (Iterables.size(contents) == 2) {
				String a = Iterables.get(contents, 0);    // a
				String b = Iterables.get(contents, 1);    // bcd

				Iterable<Iterable<String>> resB = splitAll(b, size);
				Iterable<Iterable<String>> concatAB = Iterables.transform(resB, b_ -> Iterables.concat(ImmutableList.of(a), b_));
				Iterables.addAll(res, concatAB);
			} else {
				res.add(contents);
			}
		}
		return res;
	}

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
