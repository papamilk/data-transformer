package edu.hut.common;

/**
 * 统计kpi的名称枚举类
 * @author Administrator
 *
 */
public enum KpiEnum {
	NEW_INSTALL_USER("new_install_user"),
    BROWSER_NEW_INSTALL_USER("browser_new_install_user");
	
	public final String name;

	private KpiEnum(String name) {
		this.name = name;
	}
}
