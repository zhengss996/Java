package hbase.hbase;

import hbase.util.JobRunResult;
import hbase.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * orc2hfile任务链
 * 
 * @author 郑松松
 * @time 2019年4月25日下午10:22:04
 */
public class Orc2HfileJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		// 创建任务连对象
		JobControl jobc = new JobControl("orc2hfile");
		
		Orc2Hfile orc = new Orc2Hfile();
		// 设置conf对象， 只能设置一次
		orc.setConf(conf);
		
		ControlledJob orcJob = orc.getControlledJob();
		
		jobc.addJob(orcJob);
		
		JobRunResult result = JobRunUtil.run(jobc);
		result.print(false);
		
		
//第三种导入方式： 仅此一段，上传到集群的时候使用
//		将hbase导入集成到orc2hfile任务链上，参考 hbase-shell-1.3.1.jar 导入命令
//		completebulkload /user/hadoop/hbase/output/orc2hfile_0221 user_install_status
		String outputPath = orc.getJobOutputPath(orc.getJobNameWithTaskid()).toString();
		String name = Orc2Hfile.tableName.toString();
		String[] params = new String[]{outputPath, name};
		LoadIncrementalHFiles.main(params);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
//		运行参数：(读出来)  -Dtask.input.dir=/tmp/hbase/input -Dtask.id=0425 -Dtask.base.dir=/tmp/hbase/
//		运行参数：(写出来)  -Dtask.input.dir=/tmp/hbase/input -Dtask.id=0423 -Dtask.base.dir=/tmp/hbase/ -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
		ToolRunner.run(new Orc2HfileJob(), args);
	}

}
