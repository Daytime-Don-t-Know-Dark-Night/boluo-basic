package com.boluo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

/**
 * @Author dingc
 * @Date 2022/1/7 22:01
 */
public class Func {

	private static final ObjectMapper mapper = new ObjectMapper();

	@Test
	public void func1() {
		ObjectNode obj = mapper.createObjectNode();
		obj.put("name", "dingc");
		obj.put("age", "20");
		System.out.println(obj);
	}

}
