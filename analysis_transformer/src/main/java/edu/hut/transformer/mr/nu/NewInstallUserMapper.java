package edu.hut.transformer.mr.nu;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import edu.hut.common.DateEnum;
import edu.hut.common.EventLogsConstants;
import edu.hut.common.KpiEnum;
import edu.hut.transformer.model.dim.StatsCommonDimension;
import edu.hut.transformer.model.dim.StatsUserDimension;
import edu.hut.transformer.model.dim.base.BrowserDimension;
import edu.hut.transformer.model.dim.base.DateDimension;
import edu.hut.transformer.model.dim.base.KpiDimension;
import edu.hut.transformer.model.dim.base.PlatformDimension;
import edu.hut.transformer.model.value.map.TimeOutputValue;

/**
 * 计算新用户的mapper类
 * 
 * @author Administrator
 * 
 */
public class NewInstallUserMapper extends
		TableMapper<StatsUserDimension, TimeOutputValue> {
	private static final Logger logger = Logger
			.getLogger(NewInstallUserMapper.class);
	private StatsUserDimension statsUserDimension = new StatsUserDimension();
	private TimeOutputValue timeOutputValue = new TimeOutputValue();
	private byte[] family = Bytes
			.toBytes(EventLogsConstants.EVENT_LOGS_FAMILY_NAME);
	private KpiDimension newInstallUserKpi = new KpiDimension(
			KpiEnum.NEW_INSTALL_USER.name);
	private KpiDimension newInstallUserByBrowserKpi = new KpiDimension(
			KpiEnum.BROWSER_NEW_INSTALL_USER.name);

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		String uuid = Bytes.toString(value.getValue(family,
				Bytes.toBytes(EventLogsConstants.LOG_COLUMN_NAME_UUID)));
		String serverTime = Bytes.toString(value.getValue(family,
				Bytes.toBytes(EventLogsConstants.LOG_COLUMN_NAME_SERVER_TIME)));
		String platform = Bytes.toString(value.getValue(family,
				Bytes.toBytes(EventLogsConstants.LOG_COLUMN_NAME_PLATFORM)));
		if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime)) {
			logger.warn("uuid&serverTime不能为空...");
			return;
		}
		long time = Long.valueOf(serverTime.trim());
		timeOutputValue.setId(uuid);
		timeOutputValue.setTime(time);
		DateDimension dateDimension = DateDimension.getDimensionByType(time,
				DateEnum.DAY);
		List<PlatformDimension> platformDimensions = PlatformDimension
				.list(platform);

		// 设置Date维度信息
		StatsCommonDimension statsCommonDimension = this.statsUserDimension
				.getStatsCommon();
		statsCommonDimension.setDate(dateDimension);
		// 浏览器维度信息
		String browserName = Bytes
				.toString(value.getValue(
						family,
						Bytes.toBytes(EventLogsConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
		String browserVersion = Bytes.toString(value.getValue(family, Bytes
				.toBytes(EventLogsConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
		List<BrowserDimension> browserDimensions = BrowserDimension.list(
				browserName, browserVersion);
		BrowserDimension defaultBrowser = new BrowserDimension("", "");

		for (PlatformDimension pf : platformDimensions) {
			statsUserDimension.setBrowser(defaultBrowser);
			statsCommonDimension.setKpi(newInstallUserKpi);
			statsCommonDimension.setPlatform(pf);
			context.write(statsUserDimension, timeOutputValue);

			for (BrowserDimension br : browserDimensions) {
				statsCommonDimension.setKpi(newInstallUserByBrowserKpi);
				statsUserDimension.setBrowser(br);
				context.write(statsUserDimension, timeOutputValue);
			}
		}

	}
}
