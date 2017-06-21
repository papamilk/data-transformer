package edu.hut.transformer.mr;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

import edu.hut.transformer.model.dim.base.BaseDimension;
import edu.hut.transformer.model.value.BaseStatsValueWritable;
import edu.hut.transformer.service.IDimensionConverter;

/**
 * 配合output类进行具体sql输出的类
 * 
 * @author Administrator
 * 
 */
public interface IOutputCollector {
	/**
	 * 具体执行统计数据插入的方法
	 * 
	 * @param conf
	 * @param key
	 * @param value
	 * @param pstmt
	 * @param converter
	 * @throws SQLException
	 * @throws IOException
	 */
	public void collect(Configuration conf, BaseDimension key,
			BaseStatsValueWritable value, PreparedStatement pstmt,
			IDimensionConverter converter) throws SQLException, IOException;
}
