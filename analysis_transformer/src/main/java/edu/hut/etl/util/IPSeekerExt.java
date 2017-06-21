package edu.hut.etl.util;

import edu.hut.etl.util.ip.IPSeeker;


/**
 * 具体的IP解析类，最终调用IPSeeker（父类）<br/>
 * 解析IP最终返回值是：国家 省份 城市 <br/>
 * 如果是国外IP，那么直接设置为unknown unknown unknown <br/>
 * 如果是国内IP，没法解析的话，那么就设置为中国 unknown unknown <br/>
 * 
 * @author Administrator
 * 
 */
public class IPSeekerExt extends IPSeeker {
	// 格式为：unknown unkown unkown 的RegionInfo对象
	private RegionInfo DEFAULT_REGION_INFO = new RegionInfo();

	/**
	 * 解析IP地址，返回该IP地址对应的地域信息<br/>
	 * 如果解析该IP失败，那么直接返回默认值
	 * 
	 * @param ip
	 *            要解析的IP地址
	 * @return 具体的地域信息
	 */
	public RegionInfo parseIp(String ip) {
		if (ip == null || ip.trim().isEmpty()) {
			return DEFAULT_REGION_INFO;
		}

		RegionInfo info = new RegionInfo();
		try {
			// country的格式为：xxx省(xxx市)(xxx县/区)
			String country = super.getCountry(ip).trim();
			if ("局域网".equals(country)) { // 如果是局域网，那么直接设置
				info.setCountry("中国");
				info.setProvince("湖南省");
				info.setCity("株洲市");
			} else if (country != null && !country.isEmpty()) { //
				int length = country.length();
				int index = country.indexOf('省');
				if (index > 0) { // 当前IP属于23个省中的一个
					info.setCountry("中国");
					if (index == length - 1) {
						// country格式为：湖南省
						info.setProvince(country);
					} else {
						// country格式为：湖南省株洲市
						info.setProvince(country.substring(0, index + 1));
						// 判断country中是否包含相关市的信息，如果有，那么设置该值，
						// 如果没有，则为默认值(unknow)
						int index2 = country.indexOf('市', index);
						if (index2 > 0) {
							info.setCity(country.substring(index + 1,
									index2 + 1));
						}
					}
				} else { // 其他5个自治区、4个直辖市、2个特别行政区中的一个
					String flag = country.substring(0, 2); // 获取前两个字符
					switch(flag) {
					case "内蒙": 
					case "广西":
					case "西藏":
					case "宁夏":
					case "新疆":
						info.setCountry("中国");
						info.setProvince(flag);
						country = country.substring(2);
						if (country != null && !country.isEmpty()) {
							index = country.indexOf('市');
							if (index > 0) {
								info.setCity(country.substring(0, index + 1));
							}
						}
						break;
					case "上海":
					case "北京":
					case "天津":
					case "重庆":
						info.setCountry("中国");
						info.setProvince(flag + "市");
						// 在country中，直辖市有3个字符，如：上海市
						country = country.substring(3);
						if (country != null && !country.isEmpty()) {
							index = country.indexOf('区');
							if (index > 0) {
								info.setCity(country.substring(0, index + 1));
							}
						}
						break;
					case "香港":
					case "澳门":
						info.setCountry("中国");
						info.setProvince(flag + "特别行政区");
						break;
					default:
						break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return info;

	}

	/**
	 * 内部对象，用于封装ip地域信息
	 * 
	 * @author Administrator
	 * 
	 */
	public static class RegionInfo {
		public static final String DEFAULT_VALUE = "unknown"; // 默认值
		private String country = DEFAULT_VALUE; // 国家
		private String province = DEFAULT_VALUE; // 省份
		private String city = DEFAULT_VALUE; // 城市

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public String getProvince() {
			return province;
		}

		public void setProvince(String province) {
			this.province = province;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		@Override
		public String toString() {
			return "RegionInfo [country=" + country + ", province=" + province
					+ ", city=" + city + "]";
		}
	}
}
