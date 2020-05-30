package Flink;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: wangsen
 * @Date: 2020/5/29 11:03
 * @Description:
 **/
public class MyMapFunction extends RichMapFunction<Message, Tuple4<String, Map<String, Object>, Map<String, Object>,String>> {
    private static final Logger logger = LoggerFactory.getLogger(MyMapFunction.class);
    private Config kafka_config = null;
    @Override
    public void open(Configuration parameters){
        kafka_config = ConfigService.getConfig("bigData.kafka");
    }
    @Override
    public Tuple4<String, Map<String, Object>, Map<String, Object>,String> map(Message message) throws Exception {

        Tuple4<String, Map<String, Object>, Map<String, Object>,String> result = null;

        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId != -1 && size != 0) {
            List<CanalEntry.Entry> entries = message.getEntries();
            for(CanalEntry.Entry entry : entries){
                logger.info("==================== the binlog table is {}=====================",entry.getHeader().getTableName());
                if(StringUtils.isNotEmpty(entry.getHeader().getTableName()) && entry.getHeader().getTableName().equalsIgnoreCase(
                        kafka_config.getProperty("tableNames","t_cc_transferbill"))){

                    if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND){
                        continue;
                    }
                    result = new Tuple4<>();
                    final CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    final CanalEntry.EventType eventType = rowChange.getEventType();
                    final String logfileName = entry.getHeader().getLogfileName();
                    final long logfileOffset = entry.getHeader().getLogfileOffset();
                    //final String dbname = entry.getHeader().getSchemaName();
                    final String tableName = entry.getHeader().getTableName();
                    Map<String, Object> rowBefore = new HashMap<>();
                    Map<String, Object> rowAfter = new HashMap<>();
                    result.f3 = tableName;
                    for(CanalEntry.RowData rowData : rowChange.getRowDatasList()){
                        logger.info("=========logfileName:{},logfileOffset:{}=========",logfileName,logfileOffset);
                        //区分增删改查操作
                        if(eventType == CanalEntry.EventType.INSERT){
                            //插入操作
                            logger.info("=======insert=======");
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                            for (CanalEntry.Column column:afterColumnsList){
                                rowAfter.put(column.getName(),column.getValue());
                            }
                            result.f0 = "insert";
                            result.f2 = rowAfter;
                        }else if (eventType == CanalEntry.EventType.DELETE){
                            //删除操作
                            logger.info("=======delete=======");

                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column:beforeColumnsList){
                                rowBefore.put(column.getName(),column.getValue());
                            }
                            result.f0 = "delete";
                            result.f1 = rowBefore;
                        }else if (eventType == CanalEntry.EventType.UPDATE){
                            //修改操作
                            logger.info("=======update=======");

                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column:beforeColumnsList){
                                rowBefore.put(column.getName(),column.getValue());
                            }
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                            for (CanalEntry.Column column:afterColumnsList){
                                rowAfter.put(column.getName(),column.getValue());
                            }
                            result.f0 = "update";
                            result.f1 = rowBefore;
                            result.f2 = rowAfter;
                        }else if (eventType == CanalEntry.EventType.QUERY){
                            //查询操作
                            logger.info("query sql ----> " + rowChange.getSql());
                        }
                    }
                }
            }
        }
        return result;
    }
}