package edu.hut.transformer.mr.nu;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import edu.hut.common.GlobalConstants;
import edu.hut.transformer.model.dim.StatsUserDimension;
import edu.hut.transformer.model.dim.base.BaseDimension;
import edu.hut.transformer.model.value.BaseStatsValueWritable;
import edu.hut.transformer.model.value.reduce.MapWritableValue;
import edu.hut.transformer.mr.IOutputCollector;
import edu.hut.transformer.service.IDimensionConverter;

public class StatsUserNewInstallUserCollector implements IOutputCollector {

	@Override
	public void collect(Configuration conf, BaseDimension key,
			BaseStatsValueWritable value, PreparedStatement pstmt,
			IDimensionConverter converter) throws SQLException, IOException {
		StatsUserDimension statsUserDimension = (StatsUserDimension) key;
		MapWritableValue mapWritableValue = (MapWritableValue) value;
		IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));
		
		int i = 0;
		pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAM));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.addBatch();
	}
}
