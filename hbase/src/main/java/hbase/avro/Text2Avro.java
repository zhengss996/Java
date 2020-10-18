package hbase.avro;

import java.io.IOException;

import hbase.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Text2Avro extends BaseMR {
	public static Schema schema = null;
	
	public static Schema.Parser parser = new Schema.Parser();
	
	/*
	 * public class AvroKeyOutputFormat<T> extends AvroOutputFormatBase<AvroKey<T>, NullWritable>
	 * 
	 */
	public static class Text2AvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable>{
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			//在集群运行的时候，需要重新获取schema
			if(schema == null){
				parser.parse(Text2Avro.class.getResourceAsStream("/avro_schema.txt"));
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
//			8d306de050fe0d1f        com.blurb.checkout      1418911827      2       TH      photography
			String line = value.toString();
			String[] splits = line.split("\t");
			if(splits.length != 6){
				context.getCounter("hainiu", "bad line num").increment(1L);
				return ;
			}
			
			String aid = splits[0];
			String pkgname = splits[1];
			String country = splits[4];
			String gpcategory = splits[5];
			
			// 根据schema创建 一行对象
			GenericRecord record = new GenericData.Record(schema);
			//装数据
			record.put("aid", aid);
			record.put("pkgname", pkgname);
			record.put("country", country);
			record.put("gpcategory", gpcategory);
			//作为avrokey 的 参数，创建avrokey
			context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
		}
	}

	
	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, getJobNameWithTaskid());
		job.setJarByClass(Text2Avro.class);
		
		job.setMapperClass(Text2AvroMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(AvroKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		// 输出类型
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		
		//获取schema对象
		schema = parser.parse(Text2Avro.class.getResourceAsStream("/avro_schema.txt"));
		
		AvroJob.setOutputKeySchema(job, schema);
		
		FileInputFormat.addInputPath(job, getFirstJobInputPath());
		FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskid()));
		
		return job;
	}

	
	@Override
	public String getJobName() {
		
		return "tex2avro";
	}
}

