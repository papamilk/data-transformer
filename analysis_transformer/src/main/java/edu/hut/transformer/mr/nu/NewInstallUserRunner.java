package edu.hut.transformer.mr.nu;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import edu.hut.common.EventLogsConstants;
import edu.hut.common.EventLogsConstants.EventEnum;
import edu.hut.common.GlobalConstants;
import edu.hut.transformer.model.dim.StatsUserDimension;
import edu.hut.transformer.model.value.map.TimeOutputValue;
import edu.hut.transformer.model.value.reduce.MapWritableValue;
import edu.hut.transformer.mr.TransformerOutputFormat;
import edu.hut.util.TimeUtil;

public class NewInstallUserRunner implements Tool {
	private static final Logger logger = Logger
			.getLogger(NewInstallUserRunner.class);
	private Configuration conf = new Configuration();

	/**
	 * 程序入口main方法
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new NewInstallUserRunner(),
					args);
		} catch (Exception e) {
			logger.error("计算新用户的Job出现异常...", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setConf(Configuration conf) {
		conf.addResource("output-collector.xml");
		conf.addResource("query-mapping.xml");
		conf.addResource("transformer-env.xml");
		this.conf = HBaseConfiguration.create(conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		// 添加日期参数到配置
		this.addDateArgs(conf, args);

		Job job = Job.getInstance(conf, "new_install_user");

		job.setJarByClass(NewInstallUserRunner.class);
		job.setReducerClass(NewInstallUserReducer.class);
		job.setOutputKeyClass(StatsUserDimension.class);
		job.setOutputValueClass(MapWritableValue.class);
		job.setOutputFormatClass(TransformerOutputFormat.class);

		TableMapReduceUtil.initTableMapperJob(initScans(job), NewInstallUserMapper.class,
				StatsUserDimension.class, TimeOutputValue.class, job);

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * 添加日期参数到运行配置中
	 * 
	 * @param config
	 * @param args
	 */
	private void addDateArgs(Configuration config, String[] args) {
		String date = null;
		for (int i = 0; i < args.length; i++) {
			if ("-d".equals(args[i])) {
				if (i + 1 < args.length) {
					date = args[++i];
					break;
				}
			}
		}

		// 要求日期格式为：yyyy-MM-dd
		if (StringUtils.isBlank(date) || !TimeUtil.validateRunningDate(date)) {
			// date是一个无效日期，因此设置为昨天
			date = TimeUtil.getYesterday();
		}

		config.set(GlobalConstants.RUNNING_DATE_PARAM, date);
	}
	
	/**
     * 初始化scan集合
     * 
     * @param job
     * @return
     */
    private List<Scan> initScans(Job job) {
        // 时间戳+....
        Configuration conf = job.getConfiguration();
        // 获取运行时间: yyyy-MM-dd
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAM);
        long startDate = TimeUtil.parseString2Long(date);
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;

        Scan scan = new Scan();
        // 定义hbase扫描的开始rowkey和结束rowkey
        scan.setStartRow(Bytes.toBytes("" + startDate));
        scan.setStopRow(Bytes.toBytes("" + endDate));

        FilterList filterList = new FilterList();
        // 过滤数据，只分析launch事件
        filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogsConstants.EVENT_LOGS_FAMILY_NAME), Bytes.toBytes(EventLogsConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL, Bytes.toBytes(EventEnum.LAUNCH.alias)));
        // 定义mapper中需要获取的列名
        String[] columns = new String[] { EventLogsConstants.LOG_COLUMN_NAME_EVENT_NAME, EventLogsConstants.LOG_COLUMN_NAME_UUID, EventLogsConstants.LOG_COLUMN_NAME_SERVER_TIME, EventLogsConstants.LOG_COLUMN_NAME_PLATFORM, EventLogsConstants.LOG_COLUMN_NAME_BROWSER_NAME, EventLogsConstants.LOG_COLUMN_NAME_BROWSER_VERSION };
        filterList.addFilter(this.getColumnFilter(columns));

        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogsConstants.HBASE_NAME_EVENT_LOGS));
        scan.setFilter(filterList);
        return Lists.newArrayList(scan);
    }
    
    /**
     * 获取这个列名过滤的column
     * 
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filter);
    }
}
