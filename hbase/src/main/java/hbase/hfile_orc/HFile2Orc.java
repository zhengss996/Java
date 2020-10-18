package hbase.hfile_orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import hbase.base.BaseMR;
import hbase.hbase.HFileInputFormat;
import hbase.util.Constants;
import hbase.util.OrcFormat;
import hbase.util.OrcUtil;
import hbase.writable.CellItemWritable;
import hbase.writable.RowRecordWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 读取hfile文件，并进行文件的版本合并，将合并后的版本写入orc文件
 */
public class HFile2Orc extends BaseMR {
	
	public static class HFile2OrcMapper extends Mapper<ImmutableBytesWritable, KeyValue, ImmutableBytesWritable, RowRecordWritable>{
		

		@Override
		protected void map(ImmutableBytesWritable key, KeyValue kv,Context context)
				throws IOException, InterruptedException {
			
			String colName = Bytes.toString(CellUtil.cloneQualifier(kv));
			String colValue = Bytes.toString(CellUtil.cloneValue(kv));
			long timeVersion = kv.getTimestamp();
			boolean isDelete = CellUtil.isDelete(kv);
			
			String rowkey = Bytes.toString(key.get());
			if("8d30787a5ebde1f8_20141213".equals(rowkey)){
				String aa = "123";
			}
			
			//删除某个字段的所有版本
			boolean isDeleteColumns = CellUtil.isDeleteColumns(kv);
			
			CellItemWritable cellw = new CellItemWritable(colValue, timeVersion, isDelete);
			RowRecordWritable roww = new RowRecordWritable();
			
			switch(colName){
			case "pkgname" 	  : roww.setPkgname(cellw);break;
			case "type"       : roww.setType(cellw);break;
			case "uptime"     : roww.setUptime(cellw); break;
			case "country"    : roww.setCountry(cellw); break;
			case "gpcategory" : roww.setGpcategory(cellw);; break;
			//如果字段名称不存在，就是删除整行数据
			default 		  : roww.setIsdeleteRow(cellw);break;
			}
			
			context.write(key, roww);
			
			System.out.println("mapoutput==> "+ rowkey + ", " + roww);
			System.out.println("--------------------------");
		}
	}



	public static class HFile2OrcReducer extends Reducer<ImmutableBytesWritable, RowRecordWritable, NullWritable, Writable>{
		
		OrcUtil orcUtil = new OrcUtil();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			orcUtil.setOrcTypeWriteSchema(OrcFormat.SCHEMA);
		}
		
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<RowRecordWritable> values,Context context)
				throws IOException, InterruptedException {
			
			//8d3075876ac6241d_20141227,[{Ru,xx1,false}, {CN,xx2,false}, {CN, xx3, true}]
			// 合并行数据的版本
			String rowkey = Bytes.toString(key.get());
			String aid = rowkey.split("_")[0];
			
			//代码做调试用的
			if("8d3075876ac6241d".equals(aid)){
				String aa = "123";
			}
			
			List<CellItemWritable> pkgnames = new ArrayList<CellItemWritable>();
			List<CellItemWritable> uptimes = new ArrayList<CellItemWritable>();
			List<CellItemWritable> types = new ArrayList<CellItemWritable>();
			List<CellItemWritable> countries = new ArrayList<CellItemWritable>();
			List<CellItemWritable> gpcategories = new ArrayList<CellItemWritable>();
			
			for(RowRecordWritable row : values){
				//为每个字段建立一个CellItemWritable，目的是防止reduce的iterator坑
				CellItemWritable pkgnameCell = new CellItemWritable();
				CellItemWritable uptimeCell = new CellItemWritable();
				CellItemWritable typeCell = new CellItemWritable();
				CellItemWritable countryCell = new CellItemWritable();
				CellItemWritable gpcategoryCell = new CellItemWritable();
				
				//给cellwritable 装数据
				copyData(pkgnameCell,row.getPkgname());
				copyData(uptimeCell,row.getUptime());
				copyData(typeCell,row.getType());
				copyData(countryCell, row.getCountry());
				copyData(gpcategoryCell, row.getGpcategory());
				
				//装到list里
				pkgnames.add(pkgnameCell);
				uptimes.add(uptimeCell);
				types.add(typeCell);
				countries.add(countryCell);
				gpcategories.add(gpcategoryCell);
			}
			
			// 对列表进行降序排序
			Collections.sort(pkgnames);
			Collections.sort(uptimes);
			Collections.sort(types);
			Collections.sort(countries);
			Collections.sort(gpcategories);
			
			// 获取每个cell的最新属性值
			String pkgnameNewer = getNewerTimeVersionData(pkgnames);
			String uptimeNewer = getNewerTimeVersionData(uptimes);
			String typeNewer = getNewerTimeVersionData(types);
			String countryNewer = getNewerTimeVersionData(countries);
			String gpcategoryNewer = getNewerTimeVersionData(gpcategories);
			
			// 校验如果一行的所有字段都是空，就不写入文件
			if(pkgnameNewer == null && uptimeNewer == null 
					&& typeNewer == null && countryNewer == null 
					&& gpcategoryNewer == null){
				return;
			}
			
			// 重新赋值
			String pkgname = pkgnameNewer;
			String country = countryNewer;
			String gpcategory = gpcategoryNewer;
			long uptime = -1;
			uptime = uptimeNewer == null || "".equals(uptimeNewer) ? -1: Long.parseLong(uptimeNewer);
			int type = -1;
			type = typeNewer == null || "".equals(typeNewer) ? -1 : Integer.parseInt(typeNewer);
			
			// 打印
			System.out.println("aid:" 		+ aid);
			System.out.println("pkgname:" 	+ pkgname);
			System.out.println("uptime:" 	+ uptime);
			System.out.println("type:" 		+ type);
			System.out.println("country:" 	+ country);
			System.out.println("gpcategory:" + gpcategory);
			System.out.println("------------------------------");
			
			
			// 将最新版本的数据写入orc文件
			orcUtil.addAttr(aid, pkgname, uptime, type, country, gpcategory);
			
			Writable w = orcUtil.serialize();
			
			context.write(NullWritable.get(), w);
		}
		

		/**
		 * 从 list 里面获取最新的版本
		 * @param list 一个字段的所有版本
		*/
		private String getNewerTimeVersionData(List<CellItemWritable> list) {
			String valueNewer = null;
			//摘出最新版本
			for(int i=0; i < list.size(); i++){
				
				if(list.get(i).isDelete()){
					continue;
				}
				// 该版本数据不是删除的并且，时间版本号要不是初始值（> 0）
				if(!list.get(i).isDelete() && list.get(i).getTimeVersion() > 0L){
					valueNewer = list.get(i).getValue();
				}
			}
			return valueNewer;
		}

		/**
		 * 拷贝数据到目标对象
		 * @param cellwnew 拷贝到哪
		 * @param cellw 从哪拷贝
		*/
		private void copyData(CellItemWritable cellwnew, CellItemWritable cellw) {
			cellwnew.setValue(cellw.getValue());
			cellwnew.setTimeVersion(cellw.getTimeVersion());
			cellwnew.setDelete(cellw.isDelete());
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
		job.setJarByClass(HFile2Orc.class);
		
		job.setMapperClass(HFile2OrcMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(RowRecordWritable.class);
		
		job.setReducerClass(HFile2OrcReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Writable.class);
		
		job.setInputFormatClass(HFileInputFormat.class);
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		
		
// 当使用这段的时候，输入目录必须时多分区的目录结构
		Path tablePath = new Path(conf.get(Constants.HBASE_TABLE_DIR_ATTR));
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] listStatus = fs.listStatus(tablePath);
		StringBuilder sb = new StringBuilder();
		
		for(FileStatus status : listStatus){
			String filePath = status.getPath().toString();
			//在表的目录下，有带有.的目录，需要把该目录刨除
			if(status.isDirectory() && !filePath.contains(".")){
				sb.append(filePath).append("/cf,");
			}
		}
		sb.deleteCharAt(sb.length() - 1);
		System.out.println(sb.toString());
//		/tmp/hbase/input_hfile/region1/cf,/tmp/hbase/input_hfile/region2/cf
		FileInputFormat.addInputPaths(job, sb.toString());
		
		
		
//		FileInputFormat.addInputPath(job, getFirstJobInputPath());
		FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskid()));
		return job;
	}

	@Override
	public String getJobName() {
		return "hfile2orc";
	}
}

