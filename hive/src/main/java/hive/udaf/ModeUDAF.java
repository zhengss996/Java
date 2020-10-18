package hive.udaf;

/**
 * 计算众数
 *
 * @author 郑松松
 * @time   2020年9月23日 下午12:55:42
 */

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class ModeUDAF extends AbstractGenericUDAFResolver {

    /* 该方法会根据sql传入的参数数据格式指定调用哪个Evaluator进行处理 */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted.");
        }
        if (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
            throw new UDFArgumentTypeException(0, "Only Double type arguments are accepted.");
        }
        return new DoubleEvaluator();
    }

    private static class DoubleEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private StandardListObjectInspector mergeOI;

        /* 确定各个阶段输入输出参数的数据格式ObjectInspectors */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI));
            } else {
                if (parameters[0] instanceof StandardListObjectInspector) {
                    mergeOI = (StandardListObjectInspector) parameters[0];
                    inputOI = (PrimitiveObjectInspector) mergeOI.getListElementObjectInspector();
                    return ObjectInspectorUtils.getStandardObjectInspector(mergeOI);
                } else {
                    inputOI = (PrimitiveObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputOI));
                }
            }
        }

        /* 保存数据聚集结果的类 */
        private static class MyAggBuf implements AggregationBuffer {
            List<Object> container = Lists.newArrayList();
        }

        /* 重置聚集结果 */
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MyAggBuf) agg).container.clear();
        }

        /* 获取数据集结果类 */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MyAggBuf myAgg = new MyAggBuf();
            return myAgg;
        }

        /* map阶段，迭代处理输入sql传过来的列数据 */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null || parameters.length != 1) {
                return;
            }
            if (parameters[0] != null) {
                MyAggBuf myAgg = (MyAggBuf) agg;
                myAgg.container.add(ObjectInspectorUtils.copyToStandardObject(parameters[0], this.inputOI));
            }
        }

        /* map与combiner结束返回结果，得到部分数据聚集结果 */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MyAggBuf myAgg = (MyAggBuf) agg;
            List<Object> list = Lists.newArrayList(myAgg.container);
            return list;
        }

        /* combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果 */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            MyAggBuf myAgg = (MyAggBuf) agg;
            List<Object> partialResult = (List<Object>) mergeOI.getList(partial);
            for (Object ob : partialResult) {
                myAgg.container.add(ObjectInspectorUtils.copyToStandardObject(ob, this.inputOI));
            }
        }

        /* reducer阶段，输出最终结果 */
        //计算众数
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            /* 排序 */
            List<Object> container = ((MyAggBuf) agg).container;
            ArrayList<Double> doubles = new ArrayList<Double>();
            ArrayList<DoubleWritable> dou = new ArrayList<DoubleWritable>();

            for (Object o : container) {
                doubles.add(((DoubleWritable) o).get());

            }

            int sum = 1; // 累加值
            int numbers = 1;  // 最大的个数
            Collections.sort(doubles);

            if(doubles.size() == 1) {
                dou.add(new DoubleWritable(doubles.get(0)));
            }

            for(int i = 0; i < doubles.size() - 1; i++){
                if(doubles.get(i).equals(doubles.get(i+1))){
                    sum ++;
                    if(sum > container.size() / 2){
                        dou.clear();
                        dou.add(new DoubleWritable(doubles.get(i)));
                    }
                    if(numbers < sum && i == doubles.size() - 2){
                        dou.clear();
                        dou.add(new DoubleWritable(doubles.get(i)));
                        numbers = sum;
                    }else if(numbers == sum && i == doubles.size() - 2){
                        dou.add(new DoubleWritable(doubles.get(i)));
                    }
                }else {
                    if(numbers < sum) {
                        dou.clear();
                        dou.add(new DoubleWritable(doubles.get(i)));
                        numbers = sum;
                    }else if(numbers == sum){
                        dou.add(new DoubleWritable(doubles.get(i)));
                        dou.add(new DoubleWritable(doubles.get(i+1)));
                    }
                    sum = 1;
                }
            }

            /* 构建对象并返回 */
            HashSet<Object> objects = new HashSet<Object>();

            for(int i = 0;  i < dou.size(); i++){

                objects.add(dou.get(i));
            }
            return objects;
        }
    }
}


/**
 * 注册函数
 CREATE TEMPORARY FUNCTION modes AS 'com.songsong.hive.udaf.ModeUDAF';
 * 查询
 select id, modes(age) from tt group by id order by id;
*/