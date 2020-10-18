package hive.udaf;

/**
 * 计算中位数
 *
 * @author 郑松松
 * @time   2020年9月18日 下午12:55:42
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
import java.util.LinkedList;
import java.util.List;

public class MedianUDAF extends AbstractGenericUDAFResolver {

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
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            /* 排序 */
            List<Object> container = ((MyAggBuf) agg).container;
            LinkedList<Double> doubles = new LinkedList<Double>();

            for (Object o : container) {
                doubles.add(((DoubleWritable) o).get());
            }
            Collections.sort(doubles);

            /* 计算中位数 */
            DoubleWritable median;
            int size = doubles.size();
            if (size % 2 != 0) {
                int i = size / 2;
                median = new DoubleWritable(doubles.get(i));
            } else {
                int i = size / 2;
                int j = size / 2 - 1;
                median = new DoubleWritable((doubles.get(i) + doubles.get(j)) / 2);
            }

            /* 构建对象并返回 */
            ArrayList<Object> objects = new ArrayList<Object>();
            objects.add(median);
            return objects;
        }
    }
}