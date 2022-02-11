package com.boluo.report;

import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Test;

public class ReportsTest {

	@Test
	public void splitContentTest() {
		String context = "君不见黄河之水天上来，奔流到海不复回。\n" +
				"君不见高堂明镜悲白发，朝如青丝暮成雪。\n" +
				"人生得意须尽欢，莫使金樽空对月。\n" +
				"天生我材必有用，千金散尽还复来。\n" +
				"烹羊宰牛且为乐，会须一饮三百杯。\n" +
				"岑夫子，丹丘生，将进酒，君莫停。\n" +
				"与君歌一曲，请君为我侧耳听。\n" +
				"钟鼓馔玉不足贵，但愿长醉不愿醒。\n" +
				"古来圣贤皆寂寞，惟有饮者留其名。\n" +
				"陈王昔时宴平乐，斗酒十千恣欢谑。\n" +
				"主人何为言少钱，径须沽取对君酌。\n" +
				"五花马，千金裘，呼儿将出换美酒，与尔同销万古愁。";
		int maxSize = 75;
		Iterable<String> res = Reports.splitContent(context, maxSize);
		for (String str : res) {
			System.out.println(str);
			System.out.println("--- --- ---");
		}
	}

	@Test
	public void splitContentAllTest() {
		Iterable<Iterable<String>> t = Reports.splitContentAll("1\n2\n3\n4\n5", 3);
		for (Iterable<String> i : t) {
			for (String j : i) {
				System.out.print(StringEscapeUtils.escapeJava(j) + "   ");
			}
			System.out.print('\n');
		}
	}

}
