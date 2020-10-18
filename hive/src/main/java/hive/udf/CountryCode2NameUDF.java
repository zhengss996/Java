package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * 将国家码转成国家名的自定义UDF
 *
 * @author 郑松松
 * @time   2019年8月10日 上午7:05:23
 */
public class CountryCode2NameUDF extends GenericUDF{

	// 静态加载国家字节码
	static Map<String, String> countryMap = new HashMap<String, String>();
	static{
		try(
				InputStream is = CountryCode2NameUDF.class.getResourceAsStream("/country_dict.dat");
				BufferedReader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
			)
		{
			String line = null;
			while((line = reader.readLine()) != null){
				String[] splits = line.split("\t");
				if(splits.length != 3){
					continue;
				}
				// CN 中国
				countryMap.put(splits[0], splits[1]);
			}
			System.out.println("countryMap.size:" + countryMap.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 函数输入参数校验
	 * 设定函数的返回值类型
	 */
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		// 1. 校验参数的个数  （ CN -> 中国）
		if(arguments.length != 1){
			throw new UDFArgumentException("input param must one");
		}

		// 2.校验参数的类型
		// 函数输入的参数是个字符串，而字符串属于 PRIMITIVE(基础类型)，如果是其他的类型就报错
		// PRIMITIVE, LIST, MAP, STRUCT, UNION
		if(!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
			throw new UDFArgumentException("input param Category must PRIMITIVE");
		}

		// 3. 校验参数的具体类型
		// 校验是不是基础类型中的String, 如果是其他类型就报错
		if(!arguments[0].getTypeName().equalsIgnoreCase(PrimitiveObjectInspector.PrimitiveCategory.STRING.name())){
			throw new UDFArgumentException("input param PRIMITIVE must string");
		}

		// 4. 设置函数的返回值类型, 设定函数返回类型是个字符串
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	/**
	 * 函数的核心算法
	 */
	Text output = new Text();
	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Object object = arguments[0].get();
		Text inputParam = null;

		if(object instanceof Text){
			inputParam = (Text)object;
		} else if (object instanceof LazyString) {
			LazyString lz = (LazyString)object;
			inputParam = lz.getWritableObject();
		}

		String code = inputParam.toString();
		String name = countryMap.get(code);
		name = (name == null) ? "小国家" : name;
		output.set(name);

		return output;
	}


	/**
	 * 帮助信息
	 */
	@Override
	public String getDisplayString(String[] children) {
		return "国家嘛  转  国家名称";
	}

}
