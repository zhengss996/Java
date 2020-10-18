package hive.udaf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

/**
 * @author 郑松松
 * @time   2019年8月10日 上午8:21:37
 */
public class SumUDAF extends AbstractGenericUDAFResolver{

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
		// 1. 校验输入参数
		if(info.length != 1) {
			throw new SemanticException("input param must one");
		}
		if(!info[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
			throw new SemanticException("input param Category must PRIMITIVE");
		}
		if(!info[0].getTypeName().equalsIgnoreCase(PrimitiveCategory.INT.name())){
			throw new SemanticException("input param PRIMITIVE type must int");
		}
		// 2. 返回一个算子对象
		return new SumEvaluator();
	}
	
	public static class SumEvaluator extends GenericUDAFEvaluator{
		
		// 创建一个 bean 类
		public static class SumAgg extends AbstractAggregationBuffer{
			// 存放中间结果
			private int sum;

			public int getSum() {
				return sum;
			}

			public void setSum(int sum) {
				this.sum = sum;
			}
		}
		
		// 用于mapreduce间传输的对象
		IntWritable transfer = new IntWritable();
		// 用于最终输出的对象
		IntWritable output = new IntWritable();
		

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			System.out.println("getNewAggregationBuffer()");  // 获取一个新的 Agg
			SumAgg agg = new SumAgg();
			return agg;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			System.out.println("reset()");  // 清零
			SumAgg sumAgg =(SumAgg)agg;
			sumAgg.setSum(0);
		}

		// 根据不同阶段，返回不同的结果的类型
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			System.out.println("init() ==> Mode:" + m);
			super.init(m, parameters);
			// 因为每个阶段的输出都是int类型，所以不需要每个阶段都要进行判断
			return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
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
			// 10, 20  ==> 10+20=30
			sumAgg.setSum(sumAgg.getSum() + num);
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			System.out.println("terminatePartial()");
			SumAgg sumAgg = (SumAgg)agg;
			transfer.set(sumAgg.getSum());
			return transfer;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			System.out.println("merge()");
			SumAgg sumAgg = (SumAgg)agg;
			
			IntWritable inputParam = null;
			if(partial instanceof LazyInteger){
				LazyInteger lz = (LazyInteger)partial;
				inputParam = lz.getWritableObject();
			}else if(partial instanceof IntWritable){
				inputParam = (IntWritable)partial;
			}
			int num = inputParam.get();
			// 10, 20  ==> 10+20=30
			sumAgg.setSum(sumAgg.getSum() + num);
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			System.out.println("terminate()");
			SumAgg sumAgg = (SumAgg)agg;
			output.set(sumAgg.getSum());
			return output;
		}
	}
}
