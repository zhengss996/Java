package hbase.avro;

import java.io.IOException;

import hbase.base.BaseMR;
import hbase.util.OrcUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Avro2Orc extends BaseMR {
	public static Schema schema = null;
	
	public static Schema.Parser parser = new Schema.Parser();
	
	public static class Avro2OrcMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, NullWritable, Writable>{
		OrcUtil orcUtil = new OrcUtil();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			String schema = "struct<aid:string,pkgname:string,country:string,gpcategory:string>";
			orcUtil.setOrcTypeWriteSchema(schema);
			
		}
		
		@Override
		protected void map(AvroKey<GenericRecord> key, NullWritable value,Context context)
				throws IOException, InterruptedException {
			GenericRecord datum = key.datum();
			String aid = (String)datum.get("aid");
			String pkgname = (String)datum.get("pkgname");
			String country = (String)datum.get("country");
			String gpcategory = (String)datum.get("gpcategory");
			
			
			System.out.println("aid:" 		 + aid);
			System.out.println("pkgname:" 	 + pkgname);
			System.out.println("country:" 	 + country);
			System.out.println("gpcategory:" + gpcategory);
			System.out.println("------------------------------");
			
			orcUtil.addAttr(aid, pkgname, country, gpcategory);
			
			Writable w = orcUtil.serialize();
			context.write(NullWritable.get(), w);
		}
	}

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, getJobNameWithTaskid());
		job.setJarByClass(Avro2Orc.class);
		
		job.setMapperClass(Avro2OrcMapper.class);
		
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Writable.class);
		
		job.setOutputFormatClass(OrcNewOutputFormat.class);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		
		//获取schema对象
		schema = parser.parse(Text2Avro.class.getResourceAsStream("/avro_schema.txt"));
		
		AvroJob.setInputKeySchema(job, schema);
		
		FileInputFormat.addInputPath(job, getFirstJobInputPath());
		FileOutputFormat.setOutputPath(job, getJobOutputPath(getJobNameWithTaskid()));
		
		return job;
	}

	@Override
	public String getJobName() {
		return "avro2orc";
	}
}

