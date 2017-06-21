package edu.hut.etl.util.mr;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

import edu.hut.common.EventLogsConstants;
import edu.hut.common.EventLogsConstants.EventEnum;
import edu.hut.etl.util.LoggerUtil;

/**
 * 自定义数据解析Mapper类
 * 
 * @author Administrator
 * 
 */
public class AnalysisLoadDataMapper extends
		Mapper<Object, Text, NullWritable, Put> {
	private final Logger logger = Logger
			.getLogger(AnalysisLoadDataMapper.class);
	// 计数标志
	private int inputRecords, filterRecords, outputRecords;
	private byte[] family = Bytes
			.toBytes(EventLogsConstants.EVENT_LOGS_FAMILY_NAME);
	private CRC32 crc32 = new CRC32();

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		this.inputRecords++;
		this.logger.debug("解析数据：" + value);

		try {
			// 解析日志
			Map<String, String> clientInfo = LoggerUtil.handleLog(value
					.toString());

			// 过滤解析失败的数据
			if (clientInfo.isEmpty()) {
				this.filterRecords++;
				return;
			}

			// 获取事件
			String eventAlias = clientInfo
					.get(EventLogsConstants.LOG_COLUMN_NAME_EVENT_NAME);
			EventEnum event = EventEnum.valueOfAlias(eventAlias);
			switch (event) {
			case LAUNCH:
			case PAGEVIEW:
			case CHARGEREQUEST:
			case CHARGEREFUND:
			case CHARGESUCCESS:
			case EVENT:
				// 处理数据
				this.handleData(clientInfo, event, context);
				break;
			default:
				this.filterRecords++;
				this.logger.warn("该事件无法解析，事件为：" + eventAlias);
				break;
			}
		} catch (Exception e) {
			this.filterRecords++;
			this.logger.error("处理数据发送异常，数据为：" + value, e);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		logger.info("输入数据：" + this.inputRecords + "，输出数据：" + this.outputRecords
				+ "，过滤数据：" + this.filterRecords);
	}

	/**
	 * 具体处理数据的方法
	 * 
	 * @param clientInfo
	 * @param event
	 * @param context
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void handleData(Map<String, String> clientInfo, EventEnum event,
			Context context) throws IOException, InterruptedException {
		String uuid = clientInfo.get(EventLogsConstants.LOG_COLUMN_NAME_UUID);
		String memberId = clientInfo
				.get(EventLogsConstants.LOG_COLUMN_NAME_MEMBER_ID);
		String serverTime = clientInfo
				.get(EventLogsConstants.LOG_COLUMN_NAME_SERVER_TIME);

		if (StringUtils.isNotBlank(serverTime)) { // 服务器时间不能为空
			// 为了减少数据的重复，去掉UserAgent信息
			clientInfo.remove(EventLogsConstants.LOG_COLUMN_NAME_USER_AGENT);
			// 设置HBase表的rowkey
			String rowkey = this.generateRowKey(uuid, memberId, event.alias,
					serverTime);
			Put put = new Put(Bytes.toBytes(rowkey));

			for (Entry<String, String> entry : clientInfo.entrySet()) {
				if (StringUtils.isNotBlank(entry.getKey())
						&& StringUtils.isNotBlank(entry.getValue())) {
					put.add(family, Bytes.toBytes(entry.getKey()),
							Bytes.toBytes(entry.getValue()));
				}
			}

			context.write(NullWritable.get(), put);
		} else {
			this.filterRecords++;
		}
	}

	/**
	 * 根据uuid,memberId,timestamp,evetn name创建rowkey
	 * 
	 * @param uuid
	 * @param memberId
	 * @param alias
	 * @param serverTime
	 * @return
	 */
	private String generateRowKey(String uuid, String memberId, String alias,
			String serverTime) {
		StringBuilder sb = new StringBuilder();
		sb.append(serverTime).append("_");
		this.crc32.reset();
		if (StringUtils.isNotBlank(uuid)) {
			this.crc32.update(uuid.getBytes());
		}
		if (StringUtils.isNotBlank(memberId)) {
			this.crc32.update(memberId.getBytes());
		}
		this.crc32.update(alias.getBytes());
		sb.append(this.crc32.getValue() % 100000000L);
		return sb.toString();
	}
}
