package hbase.avro;

import hbase.util.JobRunResult;
import hbase.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 通过scan hbase表数据结果，导出到orc文件任务链
 * @author   潘牛                      
 * @Date	 2019年2月21日 	 
 */
public class Avro2OrcJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		//创建 任务链对象
		JobControl jobc = new JobControl("Avro2Orcjob");
		
		Avro2Orc avro = new Avro2Orc();
		//设置conf对象，只设置一次就行
		avro.setConf(conf);
		
		ControlledJob orcJob = avro.getControlledJob();
		
		jobc.addJob(orcJob);
		
		JobRunResult result = JobRunUtil.run(jobc);
		result.print(false);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
//		运行参数：-Dtask.id=0223 -Dtask.input.dir=/tmp/avro/input_avro -Dtask.base.dir=/tmp/avro
		ToolRunner.run(new Avro2OrcJob(), args);
	}
}

