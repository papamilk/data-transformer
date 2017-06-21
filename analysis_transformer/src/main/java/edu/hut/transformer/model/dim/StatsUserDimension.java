package edu.hut.transformer.model.dim;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.hut.transformer.model.dim.base.BaseDimension;
import edu.hut.transformer.model.dim.base.BrowserDimension;

/**
 * 用户分析的Dimension组合类
 * 
 * @author Administrator
 * 
 */
public class StatsUserDimension extends StatsDimension {
	private StatsCommonDimension statsCommon = new StatsCommonDimension();
	private BrowserDimension browser = new BrowserDimension();

	public static StatsUserDimension clone(StatsUserDimension dimension) {
		BrowserDimension browser = new BrowserDimension(
				dimension.browser.getBrowserName(),
				dimension.browser.getBrowserVersion());
		StatsCommonDimension statsCommon = StatsCommonDimension
				.clone(dimension.statsCommon);
		return new StatsUserDimension(statsCommon, browser);
	}

	public StatsUserDimension() {
		super();
	}

	public StatsUserDimension(StatsCommonDimension statsCommon,
			BrowserDimension browser) {
		super();
		this.statsCommon = statsCommon;
		this.browser = browser;
	}

	public StatsCommonDimension getStatsCommon() {
		return statsCommon;
	}

	public void setStatsCommon(StatsCommonDimension statsCommon) {
		this.statsCommon = statsCommon;
	}

	public BrowserDimension getBrowser() {
		return browser;
	}

	public void setBrowser(BrowserDimension browser) {
		this.browser = browser;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.statsCommon.write(out);
		this.browser.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.statsCommon.readFields(in);
		this.browser.readFields(in);
	}

	@Override
	public int compareTo(BaseDimension o) {
		if (this == o) {
			return 0;
		}

		StatsUserDimension other = (StatsUserDimension) o;
		int tmp = this.browser.compareTo(other.browser);
		if (tmp != 0) {
			return tmp;
		}
		tmp = this.statsCommon.compareTo(other.statsCommon);
		return tmp;
	}

}
