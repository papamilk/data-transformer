package edu.hut.transformer.model.value.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import edu.hut.common.KpiEnum;
import edu.hut.transformer.model.value.BaseStatsValueWritable;

public class MapWritableValue extends BaseStatsValueWritable {
	private MapWritable value = new MapWritable();
	private KpiEnum kpi;
	
	public MapWritableValue() {
		super();
	}

	public MapWritableValue(MapWritable value, KpiEnum kpi) {
		this.value = value;
		this.kpi = kpi;
	}

	public MapWritable getValue() {
		return value;
	}

	public void setValue(MapWritable value) {
		this.value = value;
	}

	public void setKpi(KpiEnum kpi) {
		this.kpi = kpi;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.value.write(out);
		WritableUtils.writeEnum(out, this.kpi);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value.readFields(in);
		this.kpi = WritableUtils.readEnum(in, KpiEnum.class);
	}

	@Override
	public KpiEnum getKpi() {
		return this.kpi;
	}
	
}
