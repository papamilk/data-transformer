package edu.hut.etl.util;

public class TestUserAgentUtil {

	public static void main(String[] args) {
		String info = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36";
		System.out.println(UserAgentUtil.parserUserAgent(info).toString());
	}

}
