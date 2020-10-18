package hbase.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import hbase.base.BaseMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Orc2Hfile extends BaseMR {
	
	public static TableName tableName = TableName.valueOf("user_install_status_import");
	
	/*
	 * 参考TextInputFormat class LineRecordReader extends RecordReader<LongWritable, Text>
	 * 读取orc文件时需要用OrcNewInputFormat
	 * 又因为 class OrcNewInputFormat extends InputFormat<NullWritable, OrcStruct>
	 * 并且 提供的方法中返回了recordreader实例，
	 * class OrcRecordReader extends RecordReader<NullWritable, OrcStruct>
	 * 
	 * public class PutSortReducer extends 
	 * Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, KeyValue>
	 * map 输出的对象类型和reduce输入的对象类型一致
	 * 
	 * ImmutableBytesWritable： 实际上就是rowkey
	 * Put：一行数据
	 */
	public static class Orc2HfileMapper extends Mapper<NullWritable, Writable, ImmutableBytesWritable, Put>{
		
		// 读取orc文件所用的对象
		StructObjectInspector inspector_r = null;
		static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			String schema = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
			// 根据上面schema字符床，获取指定类型的typeInfo , 此类型是struct
			TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
			// 根据  struct 的 typeInfo 类型， 获取能读取该类型的inspector对象， 这个对象可以读取orc 文件
			inspector_r = (StructObjectInspector)OrcStruct.createObjectInspector(typeInfo);
		}
		
		@Override
		protected void map(NullWritable key, Writable orcData,Context context)
				throws IOException, InterruptedException {
			String aid = getStructData(orcData, "aid");
			String pkgname = getStructData(orcData, "pkgname");
			// 秒数
			String uptime = getStructData(orcData, "uptime");
			String type = getStructData(orcData, "type");
			String country = getStructData(orcData, "country");
			String gpcategory = getStructData(orcData, "gpcategory");
			
			System.out.println("aid:"	 	 + aid);
			System.out.println("pkgname:"	 + pkgname);
			System.out.println("uptime:" 	 + uptime);
			System.out.println("type:"		 + type);
			System.out.println("country:" 	 + country);
			System.out.println("gpcategory:" + gpcategory);
			
			// 上面读取
			System.out.println("-------------------------------");
			// 下面写入
			
			Date date = new Date(Long.parseLong(uptime) * 1000);
			String uptimeNew = sdf.format(date);
			
			// aid_20190221
			String rowkey = aid + "_" + uptimeNew;
			
			Put put = new Put(toBytes(rowkey));
			if(pkgname != null){  // 空数据添加不进去
				put.addColumn(toBytes("cf"), toBytes("pkgname"), toBytes(pkgname));
			}
			if(uptime != null){
				put.addColumn(toBytes("cf"), toBytes("uptime"), toBytes(uptime));
			}
			if(type != null){
				put.addColumn(toBytes("cf"), toBytes("type"), toBytes(type));
			}
			if(country != null){
				put.addColumn(toBytes("cf"), toBytes("country"), toBytes(country));
			}
			if(gpcategory != null){
				put.addColumn(toBytes("cf"), toBytes("gpcategory"), toBytes(gpcategory));
			}
			
			ImmutableBytesWritable keyOut = new ImmutableBytesWritable(toBytes(rowkey));
			context.write(keyOut, put);
		}

		private String getStructData(Writable orcData, String key) {
			// 根据字段名称， 获取字段对象
			StructField structFieldRef = inspector_r.getStructFieldRef(key);
			// 根据字段对象， 在orc文件里找到字段值
			String data = String.valueOf(inspector_r.getStructFieldData(orcData, structFieldRef));
			if(data == null || "".equals(data) || "null".equals(data)){
				data = null;
			}
			return data;
		}
	}

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, getJobNameWithTaskid());
		job.setJarByClass(Orc2Hfile.class);
		
		job.setMapperClass(Orc2HfileMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		// 输出格式是orc ， 必须写
		job.setInputFormatClass(OrcNewInputFormat.class);
		
// ① 根据下面代码  写入文件的
		Configuration hbaseConf = HBaseConfiguration.create(conf);
		Connection conn = ConnectionFactory.createConnection(hbaseConf);
		HTable table = (HTable)conn.getTable(tableName);
		// 生成 Hfile文件的详细配置都在该方法中
		// 在该方法中设置了输出hfile文件格式， 以及执行那个reduceclass
		HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator());
		
		FileInputFormat.addInputPath(job, getFirstJobInputPath());
		FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskid()));
		
		return job;
	}

	@Override
	public String getJobName() {
		return "Orc2Hfile";
	}

}
