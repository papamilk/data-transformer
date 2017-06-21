package edu.hut.common;

/**
 * 全局常量类
 * @author Administrator
 *
 */
public class GlobalConstants {
	/**
     * 一天的毫秒数
     */
    public static final int DAY_OF_MILLISECONDS = 86400000;
	/**
	 * 时间参数
	 */
	public static final String RUNNING_DATE_PARAM = "running_date";
	
	/**
	 * 默认值
	 */
	public static final String DEFAULT_VALUE = "unknown";
	
	/**
     * 维度信息表中指定全部列值
     */
    public static final String VALUE_OF_ALL = "all";
    
    /**
     * 定义的output collector的前缀
     */
    public static final String OUTPUT_COLLECTOR_KEY_PREFIX = "collector_";
    
    /**
     * 批量执行的key
     */
    public static final String JDBC_BATCH_NUMBER = "mysql.batch.number";

    /**
     * 默认批量大小
     */
    public static final String DEFAULT_JDBC_BATCH_NUMBER = "500";
    
    /**
     * 指定连接的表为report
     */
    public static final String WAREHOUSE_OF_REPORT = "report";
    
    /**
     * driver 名称
     */
    public static final String JDBC_DRIVER = "mysql.%s.driver";
    
    /**
     * JDBC url
     */
    public static final String JDBC_URL = "mysql.%s.url";
    
    /**
     * username
     */
    public static final String JDBC_USERNAME = "mysql.%s.username";
    
    /**
     * password
     */
    public static final String JDBC_PASSWORD = "mysql.%s.password";
}
