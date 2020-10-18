package hbase.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * cell包含：字段值、时间戳版本、是否被删除。
 * @author   潘牛                      
 * @Date	 2019年2月23日 	 
 */
public class CellItemWritable implements Writable, Comparable<CellItemWritable>{
	
	private String value = "";         // 字段值
	private long timeVersion = 0L;     // 时间版本
	private boolean isDelete = false;  // 是否被删除
	
	public CellItemWritable() {

	}
	
	public CellItemWritable(String value, long timeVersion, boolean isDelete) {
		super();
		this.value = value;
		this.timeVersion = timeVersion;
		this.isDelete = isDelete;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(value);
		out.writeLong(timeVersion);
		out.writeBoolean(isDelete);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value = in.readUTF();
		this.timeVersion = in.readLong();
		this.isDelete = in.readBoolean();
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getTimeVersion() {
		return timeVersion;
	}

	public void setTimeVersion(long timeVersion) {
		this.timeVersion = timeVersion;
	}

	public boolean isDelete() {
		return isDelete;
	}

	public void setDelete(boolean isDelete) {
		this.isDelete = isDelete;
	}

	@Override
	public String toString() {
		return "Cell[value=" + value + ", timeVersion=" + timeVersion + ", isDelete=" + isDelete + "]";
	}

	@Override
	public int compareTo(CellItemWritable o) {
		//直接实现降序
		if(this.timeVersion > o.timeVersion){
			return -1;
		}else if(this.timeVersion == o.timeVersion){
			return 0;
		}
		return 1;
	}
}

