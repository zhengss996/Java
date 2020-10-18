package hbase.create;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HbaseForJava {
	
	// 根据hbase参数配置，创建zookeeper连接
	static Configuration conf = HBaseConfiguration.create();
	// 连接到指定表名
	static TableName tableName = TableName.valueOf("hainiu_api");
	
	public static void main(String[] args) {
//		createTable();
//		putTable();
//		putsTable();
//		getTable();
//		scanTable();
//		deleteRowTable();
//		deleteRowColumnTable();
//		deleteColumnFamily();
		deleteTable();
//		scanTable();
	}

	private static void createTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
		)
		{
			// 创建表
			HTableDescriptor table = new HTableDescriptor(tableName);
			
			// 创建列族
			HColumnDescriptor cf1 = new HColumnDescriptor(toBytes("cf1"));
			table.addFamily(cf1);
			HColumnDescriptor cf2 = new HColumnDescriptor(toBytes("cf2"));
			table.addFamily(cf2);
			
			// 检查是否已经纯在
			if(admin.tableExists(tableName)){
				System.out.println("tableName " + tableName.toString() + "exists");
				return;
			}
			
			// 建表
			admin.createTable(table);
			System.out.println("create table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private static void putTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
		)
		{
			// 添加数据
			Put put = new Put(toBytes("id01"));
			put.addColumn(toBytes("cf1"), toBytes("name"), toBytes("n1"));
			
			table.put(put);
			System.out.println("put data to table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	private static void putsTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
		)
		{
			// 添加数据
			List<Put> puts = new ArrayList<>();
			
			Put put1 = new Put(toBytes("id01"));
			put1.addColumn(toBytes("cf1"), toBytes("age"), toBytes("n1"));
			puts.add(put1);
			
			Put put2 = new Put(toBytes("id01"));
			put2.addColumn(toBytes("cf1"), toBytes("sex"), toBytes("boy"));
			puts.add(put2);
			
			table.put(puts);
			System.out.println("puts data to table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	private static void getTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
		)
		{
			Get get = new Get(toBytes("id01"));
			
			// 代表一行数据
			Result result = table.get(get);
			printResult(result);
			
			System.out.println("get data to table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	private static void printResult(Result result) {
//		<roekey, cf, column, timestamp), values
		Cell[] rawCells = result.rawCells();
//		id01  column=cf1:age, timestamp=1550934714647, value=n1
		StringBuilder sb = new StringBuilder();
		
		for (Cell cell : rawCells) {
			sb.append(Bytes.toString(CellUtil.cloneRow(cell))).append("\tcolumn=")
			.append(Bytes.toString(CellUtil.cloneFamily(cell))).append(":")
			.append(Bytes.toString(CellUtil.cloneQualifier(cell))).append(",timestamp=")
			.append(cell.getTimestamp()).append(",value=")
			.append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n");
		}
		System.out.println(sb.toString());
	}
	

	private static void scanTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
		)
		{
			Scan scan = new Scan();
			
			// 直接定位到第二行
//			scan.setStartRow(toBytes("id02"));
//			scan.setStopRow(toBytes("id03"));
			
			
//			scan.addColumn(toBytes("cf1"), toBytes("name"));
			
			scan.setMaxVersions(2);// 显示最近俩个版本的信息
			
			// 由多个行组成的
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
				printResult(result);
			}
			
			System.out.println("scan data to table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	private static void deleteRowTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
		)
		{
			Delete delete = new Delete(toBytes("id04"));
			table.delete(delete);
			
			System.out.println("delete row data success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	private static void deleteRowColumnTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
		)
		{
			Delete delete = new Delete(toBytes("id03"), 1581345352074L);
			delete.addColumn(toBytes("cf1"), toBytes("age"));
			
			table.delete(delete);
			
			System.out.println("delete row Columns data success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	private static void deleteColumnFamily() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
				)
		{
			admin.deleteColumn(tableName, toBytes("cf2"));
			
			System.out.println("delete columnFamily success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private static void deleteTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
				)
		{
			// 防御是编程
			if(!admin.tableExists(tableName)){
				return;
			}
			
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			
			System.out.println("drop table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

