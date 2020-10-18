package hbase.driver;

import hbase.hbase.CreateSplitTableImportHbaseJob;
import hbase.hbase.Orc2HfileJob;
import hbase.scan_orc.ScanTableResult2OrcJob;
import hbase.scan_orc.ScanTableResult2OrcJobNew;
import org.apache.hadoop.util.ProgramDriver;

/**
 * 打包的类	  
 */
public class Driver {
	
	public static void main(String[] args) {
		int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    try{
//	    	orc2hfile -Dtask.input.dir=/tmp/hbase/input -Dtask.id=0221 -Dtask.base.dir=/tmp/hbase -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
	    	pgd.addClass("orc2hfile", Orc2HfileJob.class, "orc2hfile 文件");
	    	
//	    	create_import_hbase -Dtask.input.dir=/tmp/hbase/input -Dtask.id=0221 -Dtask.base.dir=/tmp/hbase -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
	    	pgd.addClass("create_import_hbase", CreateSplitTableImportHbaseJob.class, "建表并将orc文件导入表中 ");
	    	
//	    	scan_orc -Dtask.id=0222 -Dtask.base.dir=/tmp/hbase -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
	    	pgd.addClass("scan_orc", ScanTableResult2OrcJob.class, "建表并将orc文件导入表中 ");
	    	
//	    	scan_orc_new -Dtask.id=0222 -Dtask.base.dir=/tmp/hbase -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
	    	pgd.addClass("scan_orc_new", ScanTableResult2OrcJobNew.class, "建表并将orc文件导入表中 ");
	    	
	    	exitCode = pgd.run(args);
	    }
	    catch (Throwable e) {
	    	e.printStackTrace();
	    }

	    System.exit(exitCode);		
	}
}

