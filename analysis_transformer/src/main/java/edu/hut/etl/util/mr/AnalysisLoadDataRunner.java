package edu.hut.etl.util.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.hut.common.EventLogsConstants;
import edu.hut.common.GlobalConstants;
import edu.hut.util.TimeUtil;

public class AnalysisLoadDataRunner implements Tool {
	private static final Logger logger = Logger
			.getLogger(AnalysisLoadDataRunner.class);
	private Configuration conf = null;
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(),new AnalysisLoadDataRunner() ,args);
		} catch (Exception e) {
			logger.error("执行日志解析出现异常...", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = HBaseConfiguration.create(conf);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// 获取配置
		Configuration config = this.getConf();
		// 添加日期参数到配置中
		this.addDateArgs(config, args);

		// 创建Job
		Job job = Job.getInstance(config, "ayalysis_log_data");
		
		// 设置本地提交job，集群运行，需要代码
        //File jarFile = EJob.createTempJar("target/classes");
        //((JobConf) job.getConfiguration()).setJar(jarFile.toString());
        // 设置本地提交job，集群运行，需要代码结束
		
		job.setJarByClass(AnalysisLoadDataRunner.class);

		// 配置Mapper
		job.setMapperClass(AnalysisLoadDataMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);

		// 配置Reducer
		// 本地运行
//		TableMapReduceUtil.initTableReducerJob(EventLogsConstants.HBASE_NAME_EVENT_LOGS,
//				null, job, null, null, null, null, false);
		// 集群运行
		TableMapReduceUtil.initTableReducerJob(EventLogsConstants.HBASE_NAME_EVENT_LOGS, null, job);
		job.setNumReduceTasks(0);

		// 设置输入路径
		this.setJobInputPaths(job);

		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;
	}

	/**
	 * 设置job的输入路径
	 * 
	 * @param job
	 */
	private void setJobInputPaths(Job job) {
		Configuration config = job.getConfiguration();
		FileSystem fs = null;

		try {
			fs = FileSystem.get(config);
			String date = config.get(GlobalConstants.RUNNING_DATE_PARAM);
			Path inPath = new Path("/logs/"
					+ TimeUtil.parseLong2String(
							TimeUtil.parseString2Long(date), "MM/dd/"));
			if (fs.exists(inPath)) {
				FileInputFormat.addInputPath(job, inPath);
			} else {
				throw new RuntimeException("文件不存在：" + inPath);
			}
		} catch (IOException e) {
			throw new RuntimeException("设置job的mapreduce输入路径出现异常...", e);
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					// nothing，不作任何处理
				}
			}
		}

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
}
