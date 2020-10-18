package hbase.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 封装一行数， 包含了 
 *  pkgname        				   CellItemWritable
 	uptime                         CellItemWritable
 	type                           CellItemWritable
 	country                        CellItemWritable
 	gpcategory                     CellItemWritable
 	isdeleteRow(把整行都删了)	       CellItemWritable
 */
public class RowRecordWritable implements Writable{
	
	CellItemWritable pkgname    = new CellItemWritable();
	CellItemWritable uptime     = new CellItemWritable();
	CellItemWritable type       = new CellItemWritable();
	CellItemWritable country    = new CellItemWritable();
	CellItemWritable gpcategory = new CellItemWritable();
	
	/**
	 * 是否删除一行
	 */
	CellItemWritable isdeleteRow = new CellItemWritable();
	

	@Override
	public void write(DataOutput out) throws IOException {
		pkgname.write(out);
		uptime.write(out);
		type.write(out);
		country.write(out);
		gpcategory.write(out);
		isdeleteRow.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		pkgname.readFields(in);
		uptime.readFields(in);
		type.readFields(in);
		country.readFields(in);
		gpcategory.readFields(in);
		isdeleteRow.readFields(in);
	}

	public CellItemWritable getPkgname() {
		return pkgname;
	}

	public void setPkgname(CellItemWritable pkgname) {
		this.pkgname = pkgname;
	}

	public CellItemWritable getUptime() {
		return uptime;
	}

	public void setUptime(CellItemWritable uptime) {
		this.uptime = uptime;
	}

	public CellItemWritable getType() {
		return type;
	}

	public void setType(CellItemWritable type) {
		this.type = type;
	}

	public CellItemWritable getCountry() {
		return country;
	}

	public void setCountry(CellItemWritable country) {
		this.country = country;
	}

	public CellItemWritable getGpcategory() {
		return gpcategory;
	}

	public void setGpcategory(CellItemWritable gpcategory) {
		this.gpcategory = gpcategory;
	}

	public CellItemWritable getIsdeleteRow() {
		return isdeleteRow;
	}

	public void setIsdeleteRow(CellItemWritable isdeleteRow) {
		this.isdeleteRow = isdeleteRow;
	}

	@Override
	public String toString() {
		return "Row[pkgname=" + pkgname + "\n uptime=" + uptime + "\ntype=" + type + "\ncountry="
				+ country + "\ngpcategory=" + gpcategory + "\nisdeleteRow=" + isdeleteRow + "]";
	}
}

