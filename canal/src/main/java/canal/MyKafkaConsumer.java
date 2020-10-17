package canal;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.alibaba.fastjson.JSON.parseObject;

public class MyKafkaConsumer {

    public static KafkaConsumer<String, String> kafkaConsumer = null;

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092");
        props.put("group.id", "group1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<String, String>(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return consumer;
    }

    public static void receive(String topicName) {
        if (kafkaConsumer == null) {
            kafkaConsumer = createKafkaConsumer();
        }
        String topic = topicName;
        // 订阅这个消费者组
        kafkaConsumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> msgList = kafkaConsumer.poll(1000);

        if(null != msgList && msgList.count() > 0){
            for (ConsumerRecord<String, String> record : msgList) {
                System.out.println("这是一个消费线程: " + record.value());
                // 拿到json文件
                String rc = record.value();
                JSONObject jsonObject = parseObject(rc);

                String schemaName = jsonObject.getString("schemaName");
                String tableName = jsonObject.getString("tableName");
                String eventType = jsonObject.getString("eventType");
                String INSERT = jsonObject.getString("INSERT");
                String UPDATE = jsonObject.getString("UPDATE");
                String DELETE = jsonObject.getString("DELETE");
                jsonObject.remove("schemaName");
                jsonObject.remove("tableName");
                jsonObject.remove("eventType");
                jsonObject.remove("INSERT");
                jsonObject.remove("UPDATE");
                jsonObject.remove("DELETE");

                String schemaName1 = schemaName + "_ods";
                String tableName1 = tableName + "_ods";

                // 判断 走那条路
                if(INSERT.equals(eventType)) {
                    insert(jsonObject, schemaName1, tableName1);
                }else if (UPDATE.equals(eventType)){
                    update(jsonObject, schemaName1, tableName1);
                }else if (DELETE.equals(eventType)){
                    delete(jsonObject, schemaName1, tableName1);
                }
                // 提交 offerset
                kafkaConsumer.commitSync();
            }
        }
    }


    public static void insert(JSONObject jsonObject, String schemaName1, String tableName1) {
        List key = new ArrayList();
        List value = new ArrayList();
        for(String strs : jsonObject.keySet()){

            String str = "`" + strs + "`";
            key.add(str);

            String valuess = jsonObject.getString(strs);
            int number;
            //?:0或1个, *:0或多个, +:1或多个
            Boolean strResult = valuess.matches("^[0-9]+(.[0-9]+)?$");
            if(strResult == true) {
                number = Integer.valueOf(valuess);
                value.add(number);
            } else {
                valuess = "'" + valuess + "'";
                value.add(valuess);
            }
        }
        String keys = StringUtils.strip(key.toString(),"[]");
        String values = StringUtils.strip(value.toString(),"[]");
        String sql = "insert into " + schemaName1 + "." + tableName1 + "(" + keys + ") values(" + values + ");";
        JDBC_Mysql.insert(sql);
    }

    public static void update(JSONObject jsonObject, String schemaName1, String tableName1) {
        String id = jsonObject.getString("id");
        jsonObject.remove("id");

        List key = new ArrayList();
        for(String strs : jsonObject.keySet()){

            // 获取key集合
            String str = "`" + strs + "`";
            String valuess = jsonObject.getString(strs);
            String va = "";
            //?:0或1个, *:0或多个, +:1或多个
            Boolean strResult = valuess.matches("^[0-9]+(.[0-9]+)?$");
            if(strResult == true) {
                va = str + " = " + Integer.valueOf(valuess);
                key.add(va);
            } else {
                valuess = "'" + valuess + "'";
                va = str + " = " + valuess;
                key.add(va);
            }
        }
        String keys = StringUtils.strip(key.toString(),"[]");
        // update category set cname = '美妆/个护清洁/宠物' where cname = '家用电器';
        String sql = "update " + schemaName1 + "." + tableName1 + " set " + keys + " where id " + "=" + id + ";";
        JDBC_Mysql.insert(sql);
    }


    public static void delete(JSONObject jsonObject, String schemaName1, String tableName1) {

        String id = jsonObject.getString("id");
        int number;
        Boolean strResult = id.matches("^[0-9]+(.[0-9]+)?$");
        if(strResult == true) {
            number = Integer.valueOf(id);

            String sql = "delete from " + schemaName1 + "." + tableName1 + " where id " + "=" + number + ";";
            JDBC_Mysql.insert(sql);
        } else {
            id = "'" + id + "'";
            String sql = "delete from " + schemaName1 + "." + tableName1 + " where id " + "=" + id + ";";
            JDBC_Mysql.insert(sql);
        }
    }
}
