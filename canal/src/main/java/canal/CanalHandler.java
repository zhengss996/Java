package canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.base.CaseFormat;
import config.MyConfig;

import java.util.List;

public class CanalHandler {


    public  static  void handle(String schemaName , String tableName ,CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        if (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType) || CanalEntry.EventType.DELETE.equals(eventType)) {
            //下单操作
            for (CanalEntry.RowData rowData : rowDatasList) {  //行集展开

                List<CanalEntry.Column> afterColumnsList;

                // 判断是获取修改前的数据还是修改后的数据
                if(CanalEntry.EventType.DELETE.equals(eventType)){
                    afterColumnsList = rowData.getBeforeColumnsList();
                }else{
                    afterColumnsList = rowData.getAfterColumnsList();
                }

                // 将一条数据的基本信息放到一个json文件中
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("schemaName", schemaName);
                jsonObject.put("tableName", tableName);
                jsonObject.put("eventType", eventType);
                jsonObject.put("INSERT", CanalEntry.EventType.INSERT);
                jsonObject.put("UPDATE", CanalEntry.EventType.UPDATE);
                jsonObject.put("DELETE", CanalEntry.EventType.DELETE);

                // 循环得到每个字段的key  value 并添加到json文件中
                for (CanalEntry.Column column : afterColumnsList) {  //列集展开
                    System.out.println(column.getName() + "=" + column.getValue());

                    // 列名转属性名
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());

                    jsonObject.put(propertyName, column.getValue()); // 获取字段值
                }

                // 推送给kafka
                MyKafkaProducer.send(MyConfig.KAFKA_CANAL_TOPIC, jsonObject.toJSONString());
                MyKafkaConsumer.receive(MyConfig.KAFKA_CANAL_TOPIC);
            }
        }




    }
}
