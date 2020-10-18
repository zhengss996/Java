package hbase.create;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;

public class SplitRegion implements SplitAlgorithm{

	@Override
	public byte[] split(byte[] start, byte[] end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[][] split(int numRegions) {
		List<byte[]> regions = new ArrayList<byte[]>();
		// set 过滤下重复的
		Set<byte[]> set = readFile();
		for (byte[] w : set) {
			regions.add(w);
		}
		// 返回类型是byte[][], 需要用带有参数的toArray
		return regions.toArray(new byte[0][]);
	}
	
	public static void main(String[] args) {
		List<String[]> list = new ArrayList<String[]>();
		list.add(new String[]{"aa", "bb"});
		list.add(new String[]{"cc", "dd"});
		list.add(new String[]{"ee", "ff"});
		
		String[][] strs = list.toArray(new String[0][]);
		for (String[] str : strs) {
			System.out.println(Arrays.toString(str));
		}
	}

	private Set<byte[]> readFile() {
		Set<byte[]> set = new HashSet<byte[]>();
		try (
				InputStream is = SplitRegion.class.getResourceAsStream("/split_data.txt");
				BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
			)
		{
			String line = null;
			while((line = reader.readLine()) != null){
				set.add(Bytes.toBytes(line));
			}
			System.out.println("set.size():" + set.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return set;
	}

	@Override
	public byte[] firstRow() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] lastRow() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setFirstRow(String userInput) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLastRow(String userInput) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] strToRow(String input) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String rowToStr(byte[] row) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String separator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setFirstRow(byte[] userInput) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLastRow(byte[] userInput) {
		// TODO Auto-generated method stub
		
	}

}
