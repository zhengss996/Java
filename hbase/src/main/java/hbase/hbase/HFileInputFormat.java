package hbase.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 读取hfile文件的Inputformat类
 * 参考着 class TextInputFormat extends FileInputFormat<LongWritable, Text>
 * TextInputFormat 提供 createRecordReader(){
 * 	retrun new LineRecordReader);
 * }
 * 
 * public class LineRecordReader extends RecordReader<LongWritable, Text>
 * 
 * ImmutableBytesWritable: rowkey
 * 
 * KeyValue : cell
 * 
 * @author   潘牛                      
 * @Date	 2019年2月22日 	 
 */
public class HFileInputFormat extends FileInputFormat<ImmutableBytesWritable, KeyValue>{

	@Override
	public RecordReader<ImmutableBytesWritable, KeyValue> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		//返回自定义的HFileRecordReader 对象
		return new HFileRecordReader(split, context);
	}
	/*
	 * initialize()：初始化用，本次没用，用构造方法代替了该方法
	 * 
	 * nextKeyValue() ： 获取下一个key value 值
	 * 
	 * getCurrentKey() ： 获取当前的key
	 * 
	 * getCurrentValue()：获取当前的value
	 * 
	 * getProgress() ： 获取当前进度
	 *  
	 * close() ： 关闭流
	 */
	public static class HFileRecordReader extends RecordReader<ImmutableBytesWritable, KeyValue> {
		/**
		 * hbase hfile Reader 对象
		 */
		Reader reader = null;
		
		/**
		 * 读取hfile文件的扫描器对象
		 */
		HFileScanner scanner = null;
		
		long count = 0L;
		
		public HFileRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
			
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			
			FileSplit fileSplit = (FileSplit) split;
			Path path = fileSplit.getPath();
			
			CacheConfig cacheConf = new CacheConfig(conf);
			
			reader = HFile.createReader(fs, path, cacheConf, conf);
			// 第一参数：是否用缓存，不用
			// 第二个参数：是否随机读取数据， 不用
			scanner = reader.getScanner(false, false);
			// 扫描器调到文件首
			scanner.seekTo();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			//扫描器在文件首
			if(count == 0){
				// 行数+1
				count++;
				return true;
			}
			// 扫描器不在文件首
			boolean nextFlag = scanner.next();
			if(nextFlag){
				count++;
			}
			return nextFlag;
		}

		@Override
		public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
			Cell cell = scanner.getKeyValue();
			
			return new ImmutableBytesWritable(CellUtil.cloneRow(cell));
		}

		@Override
		public KeyValue getCurrentValue() throws IOException, InterruptedException {
			
			return (KeyValue) scanner.getKeyValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			long entries = reader.getEntries();
			// 进度条
			return (float)count / entries;
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}
	}
}

