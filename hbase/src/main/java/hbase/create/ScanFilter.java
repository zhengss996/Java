package hbase.create;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanFilter {
	
	static Configuration conf = HBaseConfiguration.create();
	static TableName tableName = TableName.valueOf("hainiu_api");
	
	public static void main(String[] args) {
		scanTable();
		scanTableByFilterList();
		scanPageFilterList();
	}
	
	
	/**
	 * 打印输出
	 */
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
	

	/**
	 * 条件筛选
	 */
	private static void scanTable() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
		)
		{
			Scan scan = new Scan();
			
//			SingleColumnValueFilter filter = getFilterByBinaryComparator();
//			SingleColumnValueFilter filter = getFilterByRegexStrinComparator();
			SingleColumnValueFilter filter = getFilterBySubstrinComparator();
			
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
				printResult(result);
			}
			
			System.out.println("scan data to table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 按照二进制筛选
	 */
	private static SingleColumnValueFilter getFilterByBinaryComparator() {
		BinaryComparator Comparator = new BinaryComparator(Bytes.toBytes("11"));
		SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("age"), 
				CompareOp.GREATER_OR_EQUAL, Comparator);
		// true 筛选不符合条件的（不包含该值的结果也删除了）
		filter.setFilterIfMissing(true);
		return filter;
	}
	
	/**
	 * 按正则匹配
	*/
	private static SingleColumnValueFilter getFilterByRegexStrinComparator() {
		RegexStringComparator comparator = new RegexStringComparator("^n");
		SingleColumnValueFilter filter = new SingleColumnValueFilter(toBytes("cf1"), toBytes("name"), 
				CompareOp.EQUAL, comparator);
		// true 筛选不符合条件的（不包含该值的结果也删除了）
		filter.setFilterIfMissing(true);
		return filter;
	}
	
	/**
	 * 按字符串包含匹配
	*/
	private static SingleColumnValueFilter getFilterBySubstrinComparator() {
		SubstringComparator comparator = new SubstringComparator("tt");
		SingleColumnValueFilter filter = new SingleColumnValueFilter(toBytes("cf2"), toBytes("name"), 
				CompareOp.EQUAL, comparator);
		// true 筛选不符合条件的（不包含该值的结果也删除了）
		filter.setFilterIfMissing(true);
		return filter;
	}
	
	/**
	 * 多条件筛选  FilterList
	 */
	private static void scanTableByFilterList() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
		)
		{
			Scan scan = new Scan();
			FilterList filter = new FilterList(Operator.MUST_PASS_ONE);
			FilterList filters = new FilterList(Operator.MUST_PASS_ALL);
			
			SingleColumnValueFilter filter1 = getFilterByRegexStrinComparator();  // 正则
			SingleColumnValueFilter filter2 = getFilterBySubstrinComparator();  // 字符串
			SingleColumnValueFilter filter3 = getFilterByBinaryComparator();  // 二进制
			
			filter.addFilter(filter1);
			filter.addFilter(filter2);
//			scan.setFilter(filter);
			
			filters.addFilter(filter);
			filters.addFilter(filter3);
			scan.setFilter(filters);
			
			// 由多个行组成的 result
			ResultScanner scanner = table.getScanner(scan);
			for (Result result : scanner) {
				printResult(result);
			}
			
			System.out.println("scan data to table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 分页
	 */
	private static void scanPageFilterList() {
		try (
				// 获取hbase连接
				Connection conn = ConnectionFactory.createConnection(conf);
				// 获取表管理对象
				HTable table = (HTable)conn.getTable(tableName);
				)
		{
			Scan scan = new Scan();
			int pageShowNum = 2;
			PageFilter filter = new PageFilter(pageShowNum);
			
			scan.setFilter(filter);
			byte[] startRow = null;
			byte[] stopRow = null;
			
			while(true){
				if(stopRow != null){
					// stoprow + xyz ==> startrow
					startRow = Bytes.add(stopRow, toBytes("xyz"));
					// 调整startRow位置
					scan.setStartRow(startRow);
				}
				
				int currentCount = 0;
				// 由多个行组成的 result
				ResultScanner scanner = table.getScanner(scan);
				for (Result result : scanner) {
					printResult(result);
					stopRow = CellUtil.cloneRow(result.rawCells()[0]);
					currentCount++;
				}
				// 无数据可查询，代表查询结束  （每次currentCount都会清零，计算该词的个数，如果该循环的个数少于分页的个数，那就表示最后一页了）
				if(pageShowNum > currentCount){
					break;
				}
			}
			
			System.out.println("scan data to table success");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
