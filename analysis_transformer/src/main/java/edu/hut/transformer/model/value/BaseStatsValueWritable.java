package edu.hut.transformer.model.value;

import org.apache.hadoop.io.Writable;

import edu.hut.common.KpiEnum;

/**
 * 顶级的输出value类
 * 
 * @author Administrator
 * 
 */
public abstract class BaseStatsValueWritable implements Writable {
	/**
	 * 获取当前value对于的kpi值
	 * @return
	 */
	public abstract KpiEnum getKpi();
}
