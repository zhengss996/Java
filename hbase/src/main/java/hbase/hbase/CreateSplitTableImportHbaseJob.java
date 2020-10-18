package hbase.hbase;

import hbase.util.JobRunResult;
import hbase.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 建表并将orc文件转换成hfile文件，然后再导入到hbase表里</br>
 * 步骤：
 * 		1）建带有split的表</br>
 * 		2）执行任务链orc2hfile</br>
 * 		3）将任务链生成的数据导入到hbase表</br>
 * @author 郑松松
 *
 */
public class CreateSplitTableImportHbaseJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
//		Boolean CreateFlg = Boolean.parseBoolean(conf.get("create.table"));
//		if(CreateFlg){
// 创建带有预分regoin的hbase表   仅仅增加了这一项
			// user_install_status_split com.hainiuxy.hbase.SplitRegion -c 2 -f cf
			String splitTableName = Orc2Hfile.tableName.toString();
			String splitClass = "com.hainiuxy.建表查询.SplitRegion";
			String cfName = "cf";
			String[] mainParams = new String[]{splitTableName, splitClass, "-c", "2", "-f", cfName};
			RegionSplitter.main(mainParams);
//		}
		
		// 创建任务连对象
		JobControl jobc = new JobControl("orc2hfile");
		
		Orc2Hfile orc = new Orc2Hfile();
		// 设置conf对象， 只能设置一次
		orc.setConf(conf);
		
		ControlledJob orcJob = orc.getControlledJob();
		
		jobc.addJob(orcJob);
		
		JobRunResult result = JobRunUtil.run(jobc);
		result.print(false);

// 集群运行时加上，可紧接着导入hbase表
//		将hbase导入集成到orc2hfile任务链上，参考 hbase-shell-1.3.1.jar 导入命令
//		completebulkload /user/hadoop/hbase/output/orc2hfile_0221 user_install_status
		String outputPath = orc.getJobOutputPath(orc.getJobNameWithTaskid()).toString();
		String name = Orc2Hfile.tableName.toString();
		String[] params = new String[]{outputPath, name};
		LoadIncrementalHFiles.main(params);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
//		运行参数：(读出来)  -Dtask.input.dir=/tmp/hbase/input -Dtask.id=0221 -Dtask.base.dir=/tmp/hbase/
//		运行参数：(写出来)  -Dtask.input.dir=/tmp/hbase/input -Dtask.id=0221 -Dtask.base.dir=/tmp/hbase/ -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
		ToolRunner.run(new CreateSplitTableImportHbaseJob(), args);
	}

}
