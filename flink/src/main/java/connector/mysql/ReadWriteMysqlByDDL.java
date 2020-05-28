package connector.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @Author: wangsen
 * @Date: 2020/5/22 10:42
 * @Description: 这种方式只在本地测试通过，打包后运行报错。。
 **/
public class ReadWriteMysqlByDDL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamEnv,fsSettings);
        String sourceTable ="CREATE TABLE sourceTable (\n" +
                "    FTableName VARCHAR,\n" +
                "    FECName VARCHAR\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc', -- 使用 jdbc connector\n" +
                "    'connector.url' = 'jdbc:mysql://172.20.181.91:3306/owner_cloud', -- jdbc url\n" +
                "    'connector.table' = 't_bdc_splittablerule', -- 表名\n" +
                "    'connector.username' = 'wjctest', -- 用户名\n" +
                "    'connector.password' = 'CrdWj@201702', -- 密码\n" +
                "    'connector.write.flush.max-rows' = '100' -- 默认5000条，为了演示改为100条\n" +
                ")";
        tableEnvironment.sqlUpdate(sourceTable);
        String sinkTable ="CREATE TABLE sinkTable (\n" +
                "    FID VARCHAR,\n"+
                "    FRoomName VARCHAR\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc', -- 使用 jdbc connector\n" +
                "    'connector.url' = 'jdbc:mysql://172.20.181.91:3306/owner_cloud', -- jdbc url\n" +
                "    'connector.table' = 'dwd_receiptbillentry_bak20200515111353', -- 表名\n" +
                "    'connector.username' = 'wjctest', -- 用户名\n" +
                "    'connector.password' = 'CrdWj@201702', -- 密码\n" +
                "    'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为100条\n" +
                ")";

        tableEnvironment.sqlUpdate(sinkTable);
        String query = "SELECT FTableName as tableName,FECName as ecName FROM sourceTable";
        Table table = tableEnvironment.sqlQuery(query);
        table.filter("tableName === 't_cc_receiptbill'").select("'1',ecName").insertInto("sinkTable");

        streamEnv.execute();
    }
}
