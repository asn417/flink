package batch.ods;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import common.CustomTableInputFormat;
import entity.ods.ODS_ReceiptReceiverVo;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author: wangsen
 * @Date: 2020/4/14 16:34
 * @Description:
 **/
public class ODS_ReceiptReceiver_IPF extends CustomTableInputFormat<ODS_ReceiptReceiverVo> {

    private static final Logger logger = LoggerFactory.getLogger(ODS_ReceiptReceiver_IPF.class);
    //结果Tuple
    ODS_ReceiptReceiverVo receiptReceiverVo = new ODS_ReceiptReceiverVo();

    @Override
    public void configure(Configuration configuration) {
        Connection conn = null;

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        Config apolloConfig = ConfigService.getConfig("hbase");
        config.set(HConstants.ZOOKEEPER_QUORUM, apolloConfig.getProperty("hbase.zookeeper.quorum","172.20.184.17"));
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, apolloConfig.getProperty("hbase.zookeeper.property.clientPort","2181"));
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        try {
            conn = ConnectionFactory.createConnection(config);
            table = (HTable) conn.getTable(TableName.valueOf("ods_owner_cloud:ods_receiptReceiver"));
            scan = new Scan();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /***
     * @Author: wangsen
     * @Description: 处理获取的数据
     * @Date: 2020/4/14
     * @Param: [r]
     * @Return: org.apache.flink.api.java.tuple.Tuple2<java.lang.String,java.lang.String>
     **/
    @Override
    protected ODS_ReceiptReceiverVo mapResultToTuple(Result r) {
        String rowKey = Bytes.toString(r.getRow());
        receiptReceiverVo.setiD(rowKey);
        for (Cell cell:r.listCells()){
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            //2.number
            if ("FEmployeeID".equals(column)){
                receiptReceiverVo.setEmployeeID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FEmployeeName".equals(column)){//3.headID
                receiptReceiverVo.setEmployeeName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProjectID".equals(column)){//3.headID
                receiptReceiverVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FECID".equals(column)){//3.headID
                receiptReceiverVo.seteCID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("fdeletetime".equals(column)){//3.headID
                receiptReceiverVo.setDeletetime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("fisDelete".equals(column)){//3.headID
                receiptReceiverVo.setIsDelete(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return receiptReceiverVo;
    }


    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        return "ods_owner_cloud:ods_receiptReceiver";
    }

}
