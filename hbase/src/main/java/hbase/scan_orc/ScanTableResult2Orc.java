package hbase.scan_orc;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import hbase.base.BaseMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 通过scan hbase表数据结果，导出到orc文件
 * @author   潘牛                      
 * @Date	 2019年2月22日 	 
 */
public class ScanTableResult2Orc extends BaseMR {
	
	public static TableName tableName = TableName.valueOf("user_install_status_split");
	
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	/*
	 * keyin valuein 通过TableMapReduceUtil.initTableMapperJob（）方法
	 * 这个方法里有个参数是inputformatclass
	 * class TableInputFormat extends TableInputFormatBase extends InputFormat<ImmutableBytesWritable, Result>
	 * keyin: ImmutableBytesWritable
	 * keyOut:Result
	 * -------------------------
	 * keyOut valueOut 通过 
	 * class OrcNewOutputFormat extends FileOutputFormat<NullWritable, OrcSerdeRow> 
	 * 又因为OrcSerdeRow 不让引入，用 Writable代替。
	 * keyOut： NullWritable
	 * valueOut:Writable
	 * 
	 * -------------------------
	 * 又因为 TableMapReduceUtil.initTableMapperJob（Class<? extends TableMapper> mapper）；
	 * class TableMapper<KEYOUT, VALUEOUT>
     *       extends Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
     * 所以： 可以将Mapper<ImmutableBytesWritable, Result,NullWritable, Writable> 
     *      写成
     *      TableMapper<NullWritable, Writable>
	 */
	public static class ScanTableResult2OrcMapper extends TableMapper<NullWritable, Writable>{
		
		/**
		 * 写orc的inspector对象
		 */
		ObjectInspector inspector = null;
		
		List<Object> realRow = new ArrayList<Object>();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			String schema = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
			//根据schema字符串，获取指定类型的typeinfo，此次类型是struct
			TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
			//根据struct类型，创建struct类型的inspector对象用于写orc文件
			inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
		}
		
		
		@Override
		protected void map(ImmutableBytesWritable key, Result result,Context context)
				throws IOException, InterruptedException {
			//aid_yyyyMMdd
			String rowkey = Bytes.toString(key.get());
			String[] splits = rowkey.split("_");
			String aid = splits[0];
			String uptimestr = splits[1];
			long uptime = 0L;
			try {
				Date date = sdf.parse(uptimestr);
				uptime = date.getTime() / 1000;
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			String pkgname = null, country = null, gpcategory = null;
			int type = -1;
			
			for(Cell cell : result.rawCells()){
				//字段名称
				String colName = Bytes.toString(CellUtil.cloneQualifier(cell));
				//字段值
				String colValue = Bytes.toString(CellUtil.cloneValue(cell));
				
				switch(colName){
					case "pkgname" 	  : pkgname = colValue; break;
					case "type"       : type = Integer.parseInt(colValue); break;
					case "uptime"     : uptime = Long.parseLong(colValue); break;
					case "country"    : country = colValue; break;
					case "gpcategory" : gpcategory = colValue; break;
					default 		  : break;
				}
			}
			
			System.out.println("aid:" 		+ aid);
			System.out.println("pkgname:" 	+ pkgname);
			System.out.println("uptime:" 	+ uptime);
			System.out.println("type:" 		+ type);
			System.out.println("country:" 	+ country);
			System.out.println("gpcategory:" + gpcategory);
			System.out.println("------------------------------");
			
			//------------写orc文件---------------
			OrcSerde serde = new OrcSerde();
//			清空列表
			realRow.clear();
//			aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string
			realRow.add(aid);
			realRow.add(pkgname);
			realRow.add(uptime);
			realRow.add(type);
			realRow.add(country);
			realRow.add(gpcategory);
			//将数据序列化成指定的Writable类型
			Writable w = serde.serialize(realRow, inspector);
			
			context.write(NullWritable.get(), w);
		}
	}
	
	
	@Override
	public Job getJob(Configuration conf) throws IOException {
		//关闭map的推测执行，使得一个map处理 一个region的数据
		conf.set("mapreduce.map.spedulative", "false");
		//设置orc文件snappy压缩
		conf.set("orc.compress", CompressionKind.SNAPPY.name());
		//设置orc文件 有索引
		conf.set("orc.create.index", "true");
		
		Job job = Job.getInstance(conf, getJobNameWithTaskid());
		
		job.setJarByClass(ScanTableResult2Orc.class);

		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		// 输出目录
		FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskid()));
		// 扫描全表
		Scan scan = new Scan();
		
		
		TableMapReduceUtil.initTableMapperJob(tableName, scan, ScanTableResult2OrcMapper.class, NullWritable.class, Writable.class, job);
		return job;
	}

	@Override
	public String getJobName() {
		return "scan2orc";
	}
}

