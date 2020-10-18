package hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义UDTF ,实现split逻辑， 将一行的一个字段split成多行多列
 * 
 * @author 郑松松
 * @time   2019年8月10日 上午10:23:20
 */
public class SplitUDTF extends GenericUDTF{
	
	

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
		// 1. 校验参数个数
		if(argOIs.length != 1){
			throw new UDFArgumentException("input param must one");
		}
		// 2. 校验参数的类型
		// 函数输入的参数是个字符串， 而字符串是属于 PRIMITIVE (基础类型)， 如果是其他的类型就报错
		// PRIMITIVE, LIST, MAP, STRUCT, UNION
		if(!argOIs[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
			throw new UDFArgumentException("input param Category must PRIMITIVE");
		}
		// 3. 校验参数的具体类型
		// 校验是不是基础类型中的String， 如果是其他类型就报错
		if(!argOIs[0].getTypeName().equalsIgnoreCase(PrimitiveObjectInspector.PrimitiveCategory.STRING.name())){
			throw new UDFArgumentException("input param PRIMITIVE must string");
		}
		
		// 返回值类型， 复合结构
		List<String> structFieldNames = new ArrayList<String>();
		structFieldNames.add("name");
		structFieldNames.add("age");
		
		ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
	}

	
	/**
	 * 第一个元素： name
	 * 第二个元素：age
	 */
	Object[] objs = new Object[]{new Text(), new IntWritable()};
	
	@Override
	public void process(Object[] args) throws HiveException {
		Text inputParam = null;
		if(args[0] instanceof LazyString){
			LazyString lz = (LazyString)args[0];
			inputParam = lz.getWritableObject();
		}else if(args[0] instanceof Text){
			inputParam = (Text)args[0];
		}
		String line = inputParam.toString();
		//["s1:11";"s2:12"]
		String[] splits = line.split(";");
		//word:"s1:11"
		for (String word : splits) {
			String[] tmps = word.split(":");
			String name = tmps[0];
			int age = Integer.parseInt(tmps[1]);
			((Text)objs[0]).set(name);
			((IntWritable)objs[1]).set(age);
			
			// 调用该方法输出
			forward(objs);
		}
	}

	@Override
	public void close() throws HiveException {
		
	}

}
