package edu.hut.transformer.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import edu.hut.common.DateEnum;
import edu.hut.util.TimeUtil;

/**
 * 时间维度信息类
 * 
 * @author Administrator
 * 
 */
public class DateDimension extends BaseDimension {
	private int id; // id
	private int year; // 年
	private int season; // 季度
	private int month; // 月
	private int week; // 周
	private int day; // 天
	private String type; // 类型
	private Date calendar = new Date();

	/**
	 * 根据type来获取对应的时间维度对象
	 * 
	 * @param time
	 *            时间戳
	 * @param type
	 *            DateEnum类型
	 * @return 时间维度对象
	 */
	public static DateDimension getDimensionByType(long time, DateEnum type) {
		int year = TimeUtil.getDateInfo(time, DateEnum.YEAR);
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		if (DateEnum.YEAR.equals(type)) { // 返回一个具体年份的时间维度
			// 将日期设置为year的1月1日
			calendar.set(year, 0, 1);
			return new DateDimension(year, 0, 0, 0, 0, type.name,
					calendar.getTime());
		}

		int season = TimeUtil.getDateInfo(time, DateEnum.SEASON);
		if (DateEnum.SEASON.equals(type)) { // 返回一个具体季度的时间维度
			int month = (3 * season - 2);
			calendar.set(year, month - 1, 1);
			return new DateDimension(year, season, 0, 0, 0, type.name,
					calendar.getTime());
		}

		int month = TimeUtil.getDateInfo(time, DateEnum.MONTH);
		if (DateEnum.MONTH.equals(type)) { // 返回一个具体月份的时间维度
			calendar.set(year, month - 1, 1);
			return new DateDimension(year, season, month, 0, 0, type.name,
					calendar.getTime());
		}

		int week = TimeUtil.getDateInfo(time, DateEnum.WEEK);
		if (DateEnum.WEEK.equals(type)) { // 返回一个第几周的时间维度
			long firstDayOfWeek = TimeUtil.getFirstDayOfWeek(time);
			year = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.YEAR);
			season = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.SEASON);
			month = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.MONTH);
			week = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.WEEK);
			if (week == 1 && month == 12) {
				week = 53;
			}
			return new DateDimension(year, season, month, week, 0, type.name,
					calendar.getTime());
		}

		int day = TimeUtil.getDateInfo(time, DateEnum.DAY);
		if (DateEnum.DAY.equals(type)) { // 返回一个具体日期的时间维度
			calendar.set(year, month - 1, day);
			if (week == 1 && month == 12) {
				week = 53;
			}
			return new DateDimension(year, season, month, week, day, type.name,
					calendar.getTime());
		}

		throw new RuntimeException(
				"不支持所要求的DateEnum类型来获取DateDimension对象，DateEnum类型为" + type);
	}

	public DateDimension() {
		super();
	}

	public DateDimension(int year, int season, int month, int week, int day,
			String type) {
		super();
		this.year = year;
		this.season = season;
		this.month = month;
		this.week = week;
		this.day = day;
		this.type = type;
	}

	public DateDimension(int year, int season, int month, int week, int day,
			String type, Date calendar) {
		super();
		this.year = year;
		this.season = season;
		this.month = month;
		this.week = week;
		this.day = day;
		this.type = type;
		this.calendar = calendar;
	}

	public DateDimension(int id, int year, int season, int month, int week,
			int day, String type, Date calendar) {
		super();
		this.id = id;
		this.year = year;
		this.season = season;
		this.month = month;
		this.week = week;
		this.day = day;
		this.type = type;
		this.calendar = calendar;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getSeason() {
		return season;
	}

	public void setSeason(int season) {
		this.season = season;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getWeek() {
		return week;
	}

	public void setWeek(int week) {
		this.week = week;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Date getCalendar() {
		return calendar;
	}

	public void setCalendar(Date calendar) {
		this.calendar = calendar;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id);
		out.writeInt(this.year);
		out.writeInt(this.season);
		out.writeInt(this.month);
		out.writeInt(this.week);
		out.writeInt(this.day);
		out.writeUTF(this.type);
		out.writeLong(this.calendar.getTime());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.year = in.readInt();
		this.season = in.readInt();
		this.month = in.readInt();
		this.week = in.readInt();
		this.day = in.readInt();
		this.type = in.readUTF();
		this.calendar.setTime(in.readLong());
	}

	@Override
	public int compareTo(BaseDimension o) {
		if (this == o) {
			return 0;
		}

		DateDimension other = (DateDimension) o;
		int tmp = Integer.compare(this.id, other.id);
		if (tmp != 0) {
			return tmp;
		}

		tmp = Integer.compare(this.year, other.year);
		if (tmp != 0) {
			return tmp;
		}

		tmp = Integer.compare(this.season, other.season);
		if (tmp != 0) {
			return tmp;
		}

		tmp = Integer.compare(this.month, other.month);
		if (tmp != 0) {
			return tmp;
		}

		tmp = Integer.compare(this.week, other.week);
		if (tmp != 0) {
			return tmp;
		}

		tmp = Integer.compare(this.day, other.day);
		if (tmp != 0) {
			return tmp;
		}

		tmp = this.type.compareTo(other.type);
		return tmp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + day;
		result = prime * result + id;
		result = prime * result + month;
		result = prime * result + season;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + week;
		result = prime * result + year;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DateDimension other = (DateDimension) obj;
		if (day != other.day)
			return false;
		if (id != other.id)
			return false;
		if (month != other.month)
			return false;
		if (season != other.season)
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		if (week != other.week)
			return false;
		if (year != other.year)
			return false;
		return true;
	}
}
