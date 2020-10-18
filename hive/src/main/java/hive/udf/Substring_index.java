package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Objects;

/**
 * impala
 *
 * 字符串切割成数组，取最后一个数的方法
 */
public class Substring_index extends UDF {

    public static String evaluate (String str,String delimited,Integer index) {
        if(Objects.equals(str, null)|| Objects.equals(str, "")){
            return null;
        }

        String[] split = str.split(delimited);

        if(index>0){
            if(index>split.length){
                try {
                    throw new Exception("index下标越界异常,"+"实际切割后下标有:"+split.length+"---传入的参数下标有"+index);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return split[index-1];
        }
        if(index<0){
            if(Math.abs(index)>split.length){
                try {
                    throw new Exception("index下标越界异常,"+"实际切割后下标有:"+split.length+"---传入的参数下标有"+index);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return split[split.length+index];
        }

        return  null;
    }

}

