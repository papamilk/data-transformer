package edu.hut.transformer.service.impl;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mysql.jdbc.Statement;

import edu.hut.transformer.model.dim.base.BaseDimension;
import edu.hut.transformer.model.dim.base.BrowserDimension;
import edu.hut.transformer.model.dim.base.DateDimension;
import edu.hut.transformer.model.dim.base.PlatformDimension;
import edu.hut.transformer.service.IDimensionConverter;

public class DimensionConverterImpl implements IDimensionConverter {
	private static final Logger logger = Logger
			.getLogger(DimensionConverterImpl.class);
	private static final String DRIVER = "com.mysql.jdbc.Driver";
	private static final String URL = "jdbc:mysql://192.168.150.1:3306/report?useUnicode=true&amp;characterEncoding=utf8";
	private static final String USERNAME = "root";
	private static final String PASSWORD = "123456";
	private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
		private static final long serialVersionUID = -9130059344117142828L;

		protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
			return this.size() > 5000;
		}

	};
	
	/**
	 * 加载数据库驱动
	 */
	static {
		try {
			Class.forName(DRIVER);
		} catch (ClassNotFoundException e) {
			// nothing
		}
	}
	
	@Override
	public int getDimensionIdByValue(BaseDimension dimension)
			throws IOException {
		// 创建dimension对象的cache key
		String cacheKey = this.buildCacheKey(dimension);
		if (this.cache.containsKey(cacheKey)) {
			return this.cache.get(cacheKey);
		}
		
		Connection conn = null;
		try {
			// 1.查看数据库中是否有对于的值，有则返回
			// 2.如果没有，先插入dimension的数据，再获取id
			String[] sql = null; 
			if (dimension instanceof DateDimension) {
				sql = this.buildDateSql();
			} else if (dimension instanceof PlatformDimension) {
				sql = this.buildPlatformSql();
			} else if (dimension instanceof BrowserDimension) {
				sql = this.buildBrowserSql();
			} else {
				throw new IOException("不支持此Dimension对象ID的获取...对象为：" + dimension.getClass());
			}
			
			conn = this.getConnection();
			int id = 0;
			synchronized (this) {
				id = this.executeSql(conn, cacheKey, sql, dimension);
			}
			return id;
		} catch (Throwable e) {
			logger.error("操作数据库出现异常...", e);
			throw new IOException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					// nothing
				}
			}
		}
	}
	
	/**
	 * 具体执行sql的方法
	 * @param conn
	 * @param cacheKey
	 * @param sql
	 * @param dimension
	 * @return
	 * @throws SQLException 
	 */
	private int executeSql(Connection conn, String cacheKey, String[] sql,
			BaseDimension dimension) throws SQLException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			pstmt = conn.prepareStatement(sql[0]);
			this.setArgs(pstmt, dimension);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			}
			// 代码运行到这儿，说明该Dimension对象在数据库中不存在，进行插入
			conn.prepareStatement(sql[1], Statement.RETURN_GENERATED_KEYS);
			this.setArgs(pstmt, dimension);
			pstmt.executeUpdate();
			rs = pstmt.getGeneratedKeys();
			if (rs.next()) {
				return rs.getInt(1); 
			}
		} finally {
			if (rs != null) {
                try {
                    rs.close();
                } catch (Throwable e) {
                    // nothing
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Throwable e) {
                    // nothing
                }
            }
		}
		throw new RuntimeException("从数据库获取Dimension对象的id失败...");
	}
	
	/**
	 * 设置PreparedStatement对象的参数
	 * @param pstmt
	 * @param dimension
	 * @throws SQLException 
	 */
	private void setArgs(PreparedStatement pstmt, BaseDimension dimension) throws SQLException {
		int i = 0;
		if (dimension instanceof DateDimension) {
			DateDimension date = (DateDimension) dimension;
			pstmt.setInt(++i, date.getYear());
            pstmt.setInt(++i, date.getSeason());
            pstmt.setInt(++i, date.getMonth());
            pstmt.setInt(++i, date.getWeek());
            pstmt.setInt(++i, date.getDay());
            pstmt.setString(++i, date.getType());
            pstmt.setDate(++i, new Date(date.getCalendar().getTime()));
		} else if (dimension instanceof PlatformDimension) {
			PlatformDimension platform = (PlatformDimension) dimension;
			pstmt.setString(++i, platform.getPlatformName());
		} else if (dimension instanceof BrowserDimension) {
			BrowserDimension browser = (BrowserDimension) dimension;
			pstmt.setString(++i, browser.getBrowserName());
			pstmt.setString(++i, browser.getBrowserVersion());
		}
	}

	/**
	 * 建立数据库连接
	 * @return
	 * @throws SQLException 
	 */
	private Connection getConnection() throws SQLException {
		return DriverManager.getConnection(URL, USERNAME, PASSWORD);
	}
	
	/**
	 * 创建BrowserDimension相关的sql语句
	 * @return
	 */
	private String[] buildBrowserSql() {
		String querySql = "SELECT `id` FROM `dimension_browser` WHERE `browser_name` = ? AND `browser_version` = ?";
        String insertSql = "INSERT INTO `dimension_browser`(`browser_name`, `browser_version`) VALUES(?, ?)";
        return new String[] { querySql, insertSql };
	}
	
	/**
	 * 创建PlatformDimension相关的sql语句
	 * @return
	 */
	private String[] buildPlatformSql() {
		String querySql = "SELECT `id` FROM `dimension_platform` WHERE `platform_name` = ?";
        String insertSql = "INSERT INTO `dimension_platform`(`platform_name`) VALUES(?)";
        return new String[] { querySql, insertSql };
	}
	
	/**
	 * 创建DateDimension相关的sql语句
	 * @return
	 */
	private String[] buildDateSql() {
		String querySql = "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?";
        String insertSql = "INSERT INTO `dimension_date`(`year`, `season`, `month`, `week`, `day`, `type`, `calendar`) VALUES(?, ?, ?, ?, ?, ?, ?)";
        return new String[] { querySql, insertSql };
	}

	/**
	 * 创建指定对象的cache key
	 * @param dimension
	 * @return
	 */
	private String buildCacheKey(BaseDimension dimension) {
		StringBuilder sb = new StringBuilder();
		if (dimension instanceof DateDimension) {
			sb.append("date_dimension");
			DateDimension date = (DateDimension) dimension;
			sb.append(date.getYear()).append(date.getSeason())
					.append(date.getMonth()).append(date.getWeek())
					.append(date.getDay()).append(date.getType());
		} else if (dimension instanceof PlatformDimension) {
			sb.append("platform_dimension");
			PlatformDimension platform = (PlatformDimension) dimension;
			sb.append(platform.getPlatformName());
		} else if (dimension instanceof BrowserDimension) {
			sb.append("browser_dimension");
			BrowserDimension browser = (BrowserDimension) dimension;
			sb.append(browser.getBrowserName()).append(browser.getBrowserVersion());
		}
		
		if (sb.length() == 0) {
			throw new RuntimeException("无法创建指定Dimension对象的cache key：" + dimension.getClass());
		}

		return sb.toString();
	}

}
