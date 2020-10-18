package hbase.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

/**
 * orc读写工具类</br>
 * 读取orc文件步骤： </br>
 * 		1） setOrcTypeReadSchema();</br>
 * 		2) getOrcData();</br>
 * 
 * 写入orc文件步骤：</br>
 * 		1）setOrcTypeWriteSchema();</br>
 * 		2）调用addAttr()，添加数据</br>
 * 		3）serialize()</br>
 */
public class OrcUtil {
	
	// 读取orc文件所用的对象
	StructObjectInspector inspector_r = null;
	
	// 写orc的inspector对象
	ObjectInspector inspector_w = null;
	
	// 存储一行数据
	List<Object> realRow = new ArrayList<Object>();
	
	// 序列化用的serde对象
	OrcSerde serde = null;
	
	
	/**
	 * 根据传入的orc schema字符串，创建读取orc文件的inspector对象
	 * @param schema orc schema字符串
	*/
	public void setOrcTypeReadSchema(String schema){
		//根据schema字符串，获取指定类型的typeinfo，此次类型是struct
		TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
		//根据struct typeinfo 类型获取能读取该类型的inspector对象，这个对象可以读取orc文件
		inspector_r = (StructObjectInspector)OrcStruct.createObjectInspector(typeInfo);
	}
	
	
	/**
	 * 根据传入的orc schema字符串，创建写orc文件的inspector对象
	 * @param schema orc schema字符串
	*/
	public void setOrcTypeWriteSchema(String schema){
		//根据schema字符串，获取指定类型的typeinfo，此次类型是struct
		TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
		//根据struct类型，创建struct类型的inspector对象用于写orc文件
		inspector_w = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
	}
	
	
	/**
	 * 根据字段名称在orcData获取字段值，如果数据为空，返回Null
	 * @param orcData 存储orc数据的对象
	 * @param fieldName 字段名称
	 * @return 指定字段名称的值
	*/
	public String getOrcData(Writable orcData, String fieldName) {
		//根据字段名称获取指定字段的对象
		StructField structFieldRef = inspector_r.getStructFieldRef(fieldName);
		//在根据字段对象去orc数据里找到具体字段的值
		String data = String.valueOf(inspector_r.getStructFieldData(orcData, structFieldRef));
		if(data == null || "".equals(data) || "null".equals(data)){
			data = null;
		}
		return data;
	}

	
	/**
	 * 添加数据，并返回OrcUtil对象
	 * @param data 数据
	 * @return this
	*/
	public OrcUtil addAttr(Object... data) {
		for(Object d : data){
			realRow.add(d);
		}
		return this;
	}

	
	/**
	 * 将realRow里的数据序列化成 orc Writable
	 * @return 序列化后的对象
	*/
	public Writable serialize() {
		if(serde == null){
			serde = new OrcSerde();
		}
		Writable w = serde.serialize(realRow, inspector_w);
		//序列化完成后，重新new个list
		realRow = new ArrayList<Object>();
		return w;
	}
}

