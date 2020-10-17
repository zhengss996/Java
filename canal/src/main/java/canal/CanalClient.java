package canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) throws Exception {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("192.168.174.132", 11111), "example", "", "");

        connector.connect();
        connector.subscribe(".*\\..*"); // 订阅库
        connector.rollback(); // 把上次停止后未提交的数据回滚，因为不确定是否已处理

        while (true) {
            Message message = connector.getWithoutAck(1000);
            long batchId = message.getId();
            if (batchId == -1 || message.getEntries().isEmpty()) {
                System.out.println("sleep");
                Thread.sleep(1000);
                continue;
            }
            printEntry(message.getEntries());
            connector.ack(batchId);
        }
    }

    private static void printEntry(List<Entry> entries) throws Exception {
        // 循环每一条数据
        for (Entry entry : entries) {
            if (entry.getEntryType() != EntryType.ROWDATA) {
                continue;
            }

            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());

            //获得行集
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            EventType eventType = rowChange.getEventType();   //操作类型
            String tableName = entry.getHeader().getTableName();//表名
            String schemaName = entry.getHeader().getSchemaName();//库名
            CanalHandler.handle(schemaName, tableName, eventType, rowDatasList);

        }
    }
}
