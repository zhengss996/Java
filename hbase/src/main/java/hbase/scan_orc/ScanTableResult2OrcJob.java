package hbase.scan_orc;

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
public class ScanTableResult2OrcJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		//创建 任务链对象
		JobControl jobc = new JobControl("ScanTableResult2OrcJob");
		
		ScanTableResult2Orc orc = new ScanTableResult2Orc();
		//设置conf对象，只设置一次就行
		orc.setConf(conf);
		
		ControlledJob orcJob = orc.getControlledJob();
		
		jobc.addJob(orcJob);
		
		JobRunResult result = JobRunUtil.run(jobc);
		result.print(false);
	
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
//		运行参数：-Dtask.id=0222 -Dtask.base.dir=/tmp/hbase -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
		ToolRunner.run(new ScanTableResult2OrcJob(), args);
	}
}

