package connector.mysql;

import entity.ods.ODS_ReceiptBillVo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author: wangsen
 * @Date: 2020/5/8 18:39
 * @Description:
 **/
public class ODS_ReceiptBill_IPF extends RichSourceFunction<ODS_ReceiptBillVo> {
    private static final Logger logger = LoggerFactory.getLogger(ODS_ReceiptBill_IPF.class);

    private Connection connection = null;
    private PreparedStatement ps = null;

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        connection = DriverManager.getConnection("jdbc:mysql://172.20.181.91/owner_cloud", "wjctest", "CrdWj@201702");//获取连接
        ps = connection.prepareStatement("select * from t_cc_receiptbill");
    }

    @Override
    public void run(SourceContext<ODS_ReceiptBillVo> ctx) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();

            while (resultSet.next()) {
                ODS_ReceiptBillVo receiptBillVo = new ODS_ReceiptBillVo();

                receiptBillVo.setiD(resultSet.getString("FID"));
                receiptBillVo.setNumber(resultSet.getString("FNumber"));
                receiptBillVo.setProjectID(resultSet.getString("FProjectID"));
                receiptBillVo.setOrgID(resultSet.getString("FOrgID"));
                receiptBillVo.setRoomID(resultSet.getString("FRoomID"));
                receiptBillVo.setCustomerID(resultSet.getString("FCustomerID"));
                receiptBillVo.setChequeID(resultSet.getString("FChequeID"));
                receiptBillVo.setChequeNumber(resultSet.getString("FChequeNumber"));
                receiptBillVo.setTranDate(resultSet.getString("FTranDate"));
                receiptBillVo.setRevAmount(resultSet.getString("FRevAmount"));
                receiptBillVo.setReceiverID(resultSet.getString("FReceiverID"));
                receiptBillVo.setBillID(resultSet.getString("FBillID"));
                receiptBillVo.setDescription(resultSet.getString("FDescription"));
                receiptBillVo.setStatus(resultSet.getString("FStatus"));
                receiptBillVo.setCreator(resultSet.getString("FCreator"));
                receiptBillVo.setCreateTime(resultSet.getString("FCreateTime"));
                receiptBillVo.seteCID(resultSet.getString("FECID"));
                receiptBillVo.setPeriod(resultSet.getString("FPeriod"));
                receiptBillVo.setBusinessType(resultSet.getString("FBusinessType"));
                receiptBillVo.setIsVoucher(resultSet.getString("FIsVoucher"));
                receiptBillVo.setCancel(resultSet.getString("FCancel"));
                receiptBillVo.setCancelTime(resultSet.getString("FCancelTime"));
                receiptBillVo.setIsSms(resultSet.getString("FIsSms"));
                receiptBillVo.setDeleteTime(resultSet.getString("FDeleteTime"));
                receiptBillVo.setIsDelete(resultSet.getString("FIsDelete"));
                receiptBillVo.setPrintCount(resultSet.getString("FPrintCount"));
                receiptBillVo.setVoucherID(resultSet.getString("FVoucherID"));
                receiptBillVo.setIsInvoiced(resultSet.getString("FIsInvoiced"));
                receiptBillVo.setSource(resultSet.getString("FSource"));
                receiptBillVo.setSourceState(resultSet.getString("FSourceState"));
                receiptBillVo.setThirdPayBillID(resultSet.getString("FThirdPayBillID"));
                receiptBillVo.setIsRefund(resultSet.getString("FIsRefund"));
                receiptBillVo.setSettEntryName(resultSet.getString("FSettEntryName"));
                receiptBillVo.setIsLock(resultSet.getString("FIsLock"));
                receiptBillVo.setLockDescription(resultSet.getString("FLockDescription"));
                receiptBillVo.setIsReducePenalty(resultSet.getString("FIsReducePenalty"));
                receiptBillVo.setIsConfirm(resultSet.getString("FIsConfirm"));
                receiptBillVo.setTradeNo(resultSet.getString("FTradeNo"));
                receiptBillVo.setBillNo(resultSet.getString("FBillNo"));
                receiptBillVo.setContributors(resultSet.getString("FContributors"));
                receiptBillVo.setContributor(resultSet.getString("FContributor"));
                receiptBillVo.setBillType(resultSet.getString("FBillType"));
                receiptBillVo.setOverRevAmount(resultSet.getString("FOverRevAmount"));
                receiptBillVo.setIdentifyPeople(resultSet.getString("FIdentifyPeople"));
                receiptBillVo.setFileIDs(resultSet.getString("FFileIDs"));
                receiptBillVo.setReceiptWriter(resultSet.getString("FReceiptWriter"));
                receiptBillVo.setReceiptWriteTime(resultSet.getString("FReceiptWriteTime"));
                receiptBillVo.setReceiptOperator(resultSet.getString("FReceiptOperator"));
                receiptBillVo.setConfirmCancelDescription(resultSet.getString("FConfirmCancelDescription"));
                receiptBillVo.setFirstStage(resultSet.getString("FFirstStage"));
                receiptBillVo.setConfirmReceiptDate(resultSet.getString("FConfirmReceiptDate"));


                ctx.collect(receiptBillVo);//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}
