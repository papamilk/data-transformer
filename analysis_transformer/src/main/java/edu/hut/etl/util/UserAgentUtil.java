package edu.hut.etl.util;

import java.io.IOException;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;

/**
 * 解析浏览器userAgent的工具类，内部调用uasparser jar
 * 
 * @author Administrator
 * 
 */
public class UserAgentUtil {
	static UASparser parser = null;

	// 初始化UASparser对象
	static {
		try {
			parser = new UASparser(OnlineUpdater.getVendoredInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 解析浏览器userAgent字符串，返回UserAgentInfo对象。<br/>
	 * 如果userAgent为空，返回null。如果解析失败，也返回null。
	 * 
	 * @param userAgent
	 *            要解析的user agent字符串
	 * @return 具体的值ֵ
	 */
	public static UserAgentInfo parserUserAgent(String userAgent) {
		UserAgentInfo result = null;
		if (!(userAgent == null || userAgent.trim().isEmpty())) {
			try {
				cz.mallat.uasparser.UserAgentInfo info = null;
				info = parser.parse(userAgent);
				result = new UserAgentInfo();
				result.setBrowserName(info.getUaFamily());
				result.setBrowserVersion(info.getBrowserVersionInfo());
				result.setOsName(info.getOsFamily());
				result.setOsVersion(info.getOsName());
			} catch (IOException e) {
				// 出现任何异常，将result直接设置为空
				result = null;
			}
		}
		return result;
	}

	/**
	 * 解析后的浏览器信息对象
	 * 
	 * @author Administrator
	 * 
	 */
	public static class UserAgentInfo {
		private String browserName; // 浏览器名称
		private String browserVersion; // 浏览器版本
		private String osName; // 操作系统名
		private String osVersion; // 操作系统版本

		public String getBrowserName() {
			return browserName;
		}

		public void setBrowserName(String browserName) {
			this.browserName = browserName;
		}

		public String getBrowserVersion() {
			return browserVersion;
		}

		public void setBrowserVersion(String browserVersion) {
			this.browserVersion = browserVersion;
		}

		public String getOsName() {
			return osName;
		}

		public void setOsName(String osName) {
			this.osName = osName;
		}

		public String getOsVersion() {
			return osVersion;
		}

		public void setOsVersion(String osVersion) {
			this.osVersion = osVersion;
		}

		@Override
		public String toString() {
			return "browserName=" + browserName + ", browserVersion="
					+ browserVersion + ", osName=" + osName + ", osVersion="
					+ osVersion;
		}

	}

}
