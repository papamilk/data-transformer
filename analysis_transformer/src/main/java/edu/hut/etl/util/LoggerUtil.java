package edu.hut.etl.util;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import edu.hut.common.EventLogsConstants;
import edu.hut.etl.util.IPSeekerExt.RegionInfo;
import edu.hut.etl.util.UserAgentUtil.UserAgentInfo;
import edu.hut.util.TimeUtil;

/**
 * 处理日志数据的工具类
 * 
 * @author Administrator
 * 
 */
public class LoggerUtil {
	private static final Logger logger = Logger.getLogger(LoggerUtil.class);
	private static IPSeekerExt seeker = new IPSeekerExt();

	public static Map<String, String> handleLog(String logText) {
		Map<String, String> clientInfo = new HashMap<String, String>();
		if (StringUtils.isNotBlank(logText)) {
			String[] splits = logText.trim().split(
					EventLogsConstants.LOG_SEPARTIOR);
			if (splits.length == 4) { // 日志格式：IP^A服务器时间^Ahost^A请求参数
				// 设置IP
				clientInfo.put(EventLogsConstants.LOG_COLUMN_NAME_IP,
						splits[0].trim());
				// 设置服务器时间
				clientInfo.put(EventLogsConstants.LOG_COLUMN_NAME_SERVER_TIME,
						String.valueOf(TimeUtil
								.parseNginxServerTime2Long(splits[1].trim())));
				// 设置请求参数
				int index = splits[3].indexOf("?");
				if (index > -1) {
					// 获取 url 中"?"后面的部分，也就是请求参数
					String requestBody = splits[3].substring(index + 1);
					// 处理请求参数
					handleRequestBody(requestBody, clientInfo);
					// 处理UserAgent
					handleUserAgent(clientInfo);
					// 处理IP地址
					handleIP(clientInfo);
				}

			}
		}

		return clientInfo;
	}

	/**
	 * 处理IP地址
	 * 
	 * @param clientInfo
	 */
	private static void handleIP(Map<String, String> clientInfo) {
		if (clientInfo.containsKey(EventLogsConstants.LOG_COLUMN_NAME_IP)) {
			String ip = clientInfo.get(EventLogsConstants.LOG_COLUMN_NAME_IP);
			RegionInfo info = seeker.parseIp(ip);
			if (info != null) {
				clientInfo.put(EventLogsConstants.LOG_COLUMN_NAME_COUNTRY,
						info.getCountry());
				clientInfo.put(EventLogsConstants.LOG_COLUMN_NAME_PROVINCE,
						info.getProvince());
				clientInfo.put(EventLogsConstants.LOG_COLUMN_NAME_CITY,
						info.getCity());
			}
		}
	}

	/**
	 * 处理浏览器的userAgent信息
	 * 
	 * @param clientInfo
	 */
	private static void handleUserAgent(Map<String, String> clientInfo) {
		if (clientInfo
				.containsKey(EventLogsConstants.LOG_COLUMN_NAME_USER_AGENT)) {
			UserAgentInfo info = UserAgentUtil.parserUserAgent(clientInfo
					.get(EventLogsConstants.LOG_COLUMN_NAME_USER_AGENT));
			if (info != null) {
				clientInfo.put(EventLogsConstants.LOG_COLUMN_NAME_BROWSER_NAME,
						info.getBrowserName());
				clientInfo.put(
						EventLogsConstants.LOG_COLUMN_NAME_BROWSER_VERSION,
						info.getBrowserVersion());
				clientInfo.put(EventLogsConstants.LOG_COLUMN_NAME_OS_NAME,
						info.getOsName());
				clientInfo.put(EventLogsConstants.LOG_COLUMN_NAME_OS_VERSION,
						info.getOsVersion());
			}
		}
	}

	/**
	 * 处理url请求参数
	 * 
	 * @param requestBody
	 * @param clientInfo
	 */
	private static void handleRequestBody(String requestBody,
			Map<String, String> clientInfo) {
		if (StringUtils.isNotBlank(requestBody)) {
			String[] params = requestBody.split("&");
			for (String param : params) {
				if (StringUtils.isNotBlank(param)) {
					int index = param.indexOf("=");
					if (index < 0) {
						logger.warn("无法解析参数：" + 
								param + "，请求参数为：" + requestBody);
						continue;
					}

					String key = param.substring(0, index);
					String value = null;
					try {
						value = URLDecoder.decode(param.substring(index + 1),
								"UTF-8");
					} catch (Exception e) {
						logger.warn("URL解码操作出现异常", e);
						continue;
					}
					if (StringUtils.isNotBlank(key)
							&& StringUtils.isNotBlank(value)) {
						clientInfo.put(key, value);
					}
				}
			}
		}
	}
}
