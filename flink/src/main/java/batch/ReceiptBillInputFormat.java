package batch;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import common.CustomTableInputFormat;
import entity.ods.ODS_ReceiptBillVo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import table.hbase.HBaseUtils;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author: wangsen
 * @Date: 2020/4/14 16:34
 * @Description:
 **/
public class ReceiptBillInputFormat extends CustomTableInputFormat<ODS_ReceiptBillVo> {

    //结果Tuple
    ODS_ReceiptBillVo receiptBillVo = new ODS_ReceiptBillVo();

    @Override
    public void configure(Configuration configuration) {

        Connection conn = null;
        Admin admin = null;
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        Config apolloConfig = ConfigService.getConfig("hbase");
        config.set(HConstants.ZOOKEEPER_QUORUM, apolloConfig.getProperty("hbase.zookeeper.quorum","172.20.184.17"));
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, apolloConfig.getProperty("hbase.zookeeper.property.clientPort","2181"));
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 3000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 3000);

        try {
            conn = ConnectionFactory.createConnection(config);
            admin = conn.getAdmin();
            if (!admin.tableExists(TableName.valueOf("ods_owner_cloud:ods_receiptBill"))){
                HBaseUtils.createNamespace("ods_owner_cloud");
                String[] keys = {"1","2","3","4","5","6","7","8","9"};
                byte[][] splitKeys = HBaseUtils.getSplitKeys(Arrays.asList(keys));
                String[] cf = {"cf"};
                HBaseUtils.createTableBySplitKeys("ods_owner_cloud:ods_receiptBill",Arrays.asList(cf),splitKeys,true);
            }
            //tableName = "ods_owner_cloud:ods_receiptBill";
            TableName tableName = TableName.valueOf("ods_owner_cloud:ods_receiptBill");
            conn = ConnectionFactory.createConnection(config);
            table = (HTable) conn.getTable(tableName);
            scan = new Scan();
            //scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("FCustomerID"));
            //scan.addFamily(Bytes.toBytes("cf"));

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
    protected ODS_ReceiptBillVo mapResultToTuple(Result r) {
        String rowKey = Bytes.toString(r.getRow());
        receiptBillVo.setiD(rowKey);
        for (Cell cell:r.listCells()){
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            //2.number
            if ("FNumber".equals(column)){
                receiptBillVo.setNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FOrgID".equals(column)){//3.headID
                receiptBillVo.setOrgID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCustomerID".equals(column)){//3.headID
                receiptBillVo.setCustomerID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FChequeID".equals(column)){//3.headID
                receiptBillVo.setChequeID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FChequeNumber".equals(column)){//3.headID
                receiptBillVo.setChequeNumber(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPeriod".equals(column)){//3.headID
                receiptBillVo.setPeriod(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FTranDate".equals(column)){//3.headID
                receiptBillVo.setTranDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FReceiverID".equals(column)){//3.headID
                receiptBillVo.setReceiverID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBillID".equals(column)){//3.headID
                receiptBillVo.setBillID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRevAmount".equals(column)){//3.headID
                receiptBillVo.setRevAmount(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDescription".equals(column)){//3.headID
                receiptBillVo.setDescription(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBusinessType".equals(column)){//3.headID
                receiptBillVo.setBusinessType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FStatus".equals(column)){//3.headID
                receiptBillVo.setStatus(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCreator".equals(column)){//3.headID
                receiptBillVo.setCreator(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCreateTime".equals(column)){//3.headID
                receiptBillVo.setCreateTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FECID".equals(column)){//3.headID
                receiptBillVo.seteCID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCancel".equals(column)){//3.headID
                receiptBillVo.setCancel(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FCancelTime".equals(column)){//3.headID
                receiptBillVo.setCancelTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsSms".equals(column)){//3.headID
                receiptBillVo.setIsSms(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FDeleteTime".equals(column)){//3.headID
                receiptBillVo.setDeleteTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsDelete".equals(column)){//3.headID
                receiptBillVo.setIsDelete(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FPrintCount".equals(column)){//3.headID
                receiptBillVo.setPrintCount(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSource".equals(column)){//3.headID
                receiptBillVo.setSource(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSourceState".equals(column)){//3.headID
                receiptBillVo.setSourceState(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FThirdPayBillID".equals(column)){//3.headID
                receiptBillVo.setThirdPayBillID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsRefund".equals(column)){//3.headID
                receiptBillVo.setIsRefund(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FSettEntryName".equals(column)){//3.headID
                receiptBillVo.setSettEntryName(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FLockDescription".equals(column)){//3.headID
                receiptBillVo.setLockDescription(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsConfirm".equals(column)){//3.headID
                receiptBillVo.setIsConfirm(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FTradeNo".equals(column)){//3.headID
                receiptBillVo.setTradeNo(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FContributors".equals(column)){//3.headID
                receiptBillVo.setContributors(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FContributor".equals(column)){//3.headID
                receiptBillVo.setContributor(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsVoucher".equals(column)){//3.headID
                receiptBillVo.setIsVoucher(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FVoucherID".equals(column)){//3.headID
                receiptBillVo.setVoucherID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBillType".equals(column)){//3.headID
                receiptBillVo.setBillType(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FOverRevAmount".equals(column)){//3.headID
                receiptBillVo.setOverRevAmount(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIdentifyPeople".equals(column)){//3.headID
                receiptBillVo.setIdentifyPeople(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFileIDs".equals(column)){//3.headID
                receiptBillVo.setFileIDs(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FReceiptWriter".equals(column)){//3.headID
                receiptBillVo.setReceiptWriter(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FReceiptWriteTime".equals(column)){//3.headID
                receiptBillVo.setReceiptWriteTime(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FRoomID".equals(column)){//3.headID
                receiptBillVo.setRoomID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FReceiptOperator".equals(column)){//3.headID
                receiptBillVo.setReceiptOperator(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FConfirmCancelDescription".equals(column)){//3.headID
                receiptBillVo.setConfirmCancelDescription(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FProjectID".equals(column)){//3.headID
                receiptBillVo.setProjectID(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsLock".equals(column)){//3.headID
                receiptBillVo.setIsLock(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FBillNo".equals(column)){//3.headID
                receiptBillVo.setBillNo(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsReducePenalty".equals(column)){//3.headID
                receiptBillVo.setIsReducePenalty(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FFirstStage".equals(column)){//3.headID
                receiptBillVo.setFirstStage(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FIsInvoiced".equals(column)){//3.headID
                receiptBillVo.setIsInvoiced(Bytes.toString(CellUtil.cloneValue(cell)));
            }else if ("FConfirmReceiptDate".equals(column)){//3.headID
                receiptBillVo.setConfirmReceiptDate(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return receiptBillVo;
    }


    @Override
    protected Scan getScanner() {
        return scan;
    }

    @Override
    protected String getTableName() {
        return "ods_owner_cloud:ods_receiptBill";
    }
}
