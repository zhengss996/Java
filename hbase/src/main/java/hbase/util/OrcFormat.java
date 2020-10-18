package hbase.util;

/**
 * 存放orc文件格式的schema字符串
 * @author   潘牛                      
 * @Date	 2019年2月22日 	 
 */
public class OrcFormat {
	public static String SCHEMA = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
}

