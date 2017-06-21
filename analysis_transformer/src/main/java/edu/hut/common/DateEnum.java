package edu.hut.common;

/**
 * 日期类型枚举类
 * 
 * @author Administrator
 * 
 */
public enum DateEnum {
	YEAR("year"), SEASON("season"), MONTH("month"), WEEK("week"), DAY("day"), HOUR(
			"hour");

	public final String name;

	private DateEnum(String name) {
		this.name = name;
	}

	/**
	 * 根据name的值获取对于的DateEnum
	 * 
	 * @param name
	 * @return
	 */
	public static DateEnum valueOfName(String name) {
		for (DateEnum type : values()) {
			if (type.name.equals(name)) {
				return type;
			}
		}
		return null;
	}
}
