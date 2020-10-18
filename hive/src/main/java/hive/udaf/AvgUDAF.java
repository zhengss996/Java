package hive.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义UDFA，实现avg()的逻辑
 * @author 郑松松
 * @time 2019年4月25日下午2:26:31
 *
 */
public class AvgUDAF extends AbstractGenericUDAFResolver{

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
		// 1. 校验输入参数
		if(info.length != 1){
			throw new SemanticException("input param must one");
		}
		if(!info[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
			throw new SemanticException("input param Category must PRIMITIVE");
		}
		if(!info[0].getTypeName().equalsIgnoreCase(PrimitiveCategory.INT.name())){
			throw new SemanticException("input param PRIVITIVE type must int");
		}
		// 2. 返回一个算子对象
		return new AvgEvaluator();
	}

	public static class AvgEvaluator extends GenericUDAFEvaluator{
		public static class SumAgg extends AbstractAggregationBuffer{
			// 存放中间结果 --> 和
			private int sum;

			public int getSum() {
				return sum;
			}
			public void setSum(int sum) {
				this.sum = sum;
			}
			
			// 存放中间结果-->计数
			private int count;

			public int getCount() {
				return count;
			}
			public void setCount(int count) {
				this.count = count;
			}
		}
		
		/**
		 * 用于mapreduce间传输的对象
		 */
		IntWritable[] transfer = new IntWritable[]{new IntWritable(), new IntWritable()};
		/**
		 * 函数最终传输的对象
		 */
		DoubleWritable output = new DoubleWritable();
		
		
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			System.out.println("getNewAggregationBuffer()");
			SumAgg agg = new SumAgg();
			return agg;
		}

		
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			System.out.println("reset()");
			SumAgg sumAgg = (SumAgg)agg;
			sumAgg.setSum(0);
			sumAgg.setCount(0);
		}

		
		// 根据不同的阶段， 返回不同的结果的类型
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			System.out.println("init() ==> Mode:" + m);
			super.init(m, parameters);
			if(m == Mode.PARTIAL1 || m == Mode.PARTIAL2){
				// 返回一个复合结构
				// 复合结构中字段名称列表
				List<String> structFieldNames = new ArrayList<String>();
				structFieldNames.add("sum");
				structFieldNames.add("count");
				
				// 复合结构中字段类型列表
				List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
				structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
				structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
				
				return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
			}
			// 在final 阶段返回double 类型
			return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		}
		
		
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			System.out.println("iterate()");
			SumAgg sumAgg = (SumAgg)agg;
			Object object = parameters[0];
			IntWritable inputParam = null;
			if(object instanceof LazyInteger){
				LazyInteger lz = (LazyInteger)object;
				inputParam = lz.getWritableObject();
			}else if(object instanceof IntWritable){
				inputParam = (IntWritable)object;
			}
			int num = inputParam.get();
			// 10, 20   ==> 10+20=30
			sumAgg.setSum(sumAgg.getSum() + num);
			sumAgg.setCount(sumAgg.getCount() + 1);
			
		}


		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			System.out.println("terminatePartial()");
			SumAgg sumAgg = (SumAgg)agg;
			// sum 
			transfer[0].set(sumAgg.getSum());
			// count
			transfer[1].set(sumAgg.getCount());
			return transfer;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			System.out.println("merge()");
			SumAgg sumAgg = (SumAgg)agg;
			
			IntWritable sumParam = null;
			IntWritable countParam = null;
			if(partial instanceof LazyBinaryStruct){
				LazyBinaryStruct lz = (LazyBinaryStruct)partial;
				sumParam = (IntWritable)lz.getField(0);
				countParam = (IntWritable)lz.getField(1);
			}
			int sum = sumParam.get();
			int count = countParam.get();
			// 10, 20   ==> 10+20=30
			sumAgg.setSum(sumAgg.getSum() + sum);
			sumAgg.setCount(sumAgg.getCount() + count);
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			System.out.println("terminate()");
			SumAgg sumAgg = (SumAgg)agg;
			
			int sum = sumAgg.getSum();
			int count = sumAgg.getCount();
			
			double avg = (double) sum / count;
			output.set(avg);
			
			return output;
		}
	}
}