package edu.hut.transformer.service;

import java.io.IOException;

import edu.hut.transformer.model.dim.base.BaseDimension;

/**
 * 操作Dimension表的接口
 * 
 * @author Administrator
 * 
 */
public interface IDimensionConverter {
	/**
	 * 根据dimension对象的值获取id<br/>
	 * 如果数据库中存在，直接返回。如果没有，那么进行插入再返回新的id值。
	 * 
	 * @param dim
	 * @return
	 * @throws IOException
	 */
	public int getDimensionIdByValue(BaseDimension dimension) throws IOException;
}
