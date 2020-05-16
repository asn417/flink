package connector.mysql;

/**
 * @Author: wangsen
 * @Date: 2020/5/8 14:50
 * @Description:
 **/

import warehouse.ods.entity.ODS_ReceiptBillEntryVo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class ODS_ReceiptBillEntry_IPF extends RichSourceFunction<ODS_ReceiptBillEntryVo> {
    private static final Logger logger = LoggerFactory.getLogger(ODS_ReceiptBillEntry_IPF.class);

    private Connection connection = null;
    private PreparedStatement ps = null;

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        connection = DriverManager.getConnection("jdbc:mysql://172.20.181.91/owner_cloud", "wjctest", "CrdWj@201702");//获取连接
        ps = connection.prepareStatement("select * from t_cc_receiptbillentry");
    }

    //执行查询并获取结果
    @Override
    public void run(SourceContext<ODS_ReceiptBillEntryVo> ctx) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();

            while (resultSet.next()) {
                ODS_ReceiptBillEntryVo receiptBillEntryVo = new ODS_ReceiptBillEntryVo();

                receiptBillEntryVo.setiD(resultSet.getString("FID"));
                receiptBillEntryVo.setNumber(resultSet.getString("FNumber"));
                receiptBillEntryVo.setHeadID(resultSet.getString("FHeadID"));
                receiptBillEntryVo.setMoneyDefineID(resultSet.getString("FMoneyDefineID"));
                receiptBillEntryVo.setMoneyStandardID(resultSet.getString("FMoneyStandardID"));
                receiptBillEntryVo.setMoneyType(resultSet.getString("FMoneyType"));
                receiptBillEntryVo.setPeriod(resultSet.getString("FPeriod"));
                receiptBillEntryVo.setReceivableDate(resultSet.getString("FReceivableDate"));
                receiptBillEntryVo.setReceivableAmount(resultSet.getString("FReceivableAmount"));
                receiptBillEntryVo.setRevPenaltyAmount(new BigDecimal(resultSet.getString("FRevPenaltyAmount")));
                receiptBillEntryVo.setRevAmount(new BigDecimal(resultSet.getString("FRevAmount")));
                receiptBillEntryVo.setDescription(resultSet.getString("FDescription"));
                receiptBillEntryVo.setBusinessType(resultSet.getString("FBusinessType"));
                receiptBillEntryVo.setStatus(resultSet.getString("FStatus"));
                receiptBillEntryVo.setCreator(resultSet.getString("FCreator"));
                receiptBillEntryVo.setCreateTime(resultSet.getString("FCreateTime"));
                receiptBillEntryVo.seteCID(resultSet.getString("FECID"));
                receiptBillEntryVo.setReceiveID(resultSet.getString("FReceiveID"));
                receiptBillEntryVo.setQty(resultSet.getString("FQty"));
                receiptBillEntryVo.setPrice(resultSet.getString("FPrice"));
                receiptBillEntryVo.setDeleteTime(resultSet.getString("FDeleteTime"));
                receiptBillEntryVo.setIsDelete(resultSet.getString("FIsDelete"));
                receiptBillEntryVo.setTaxRate(resultSet.getString("FTaxRate"));
                receiptBillEntryVo.setTaxAmount(resultSet.getString("FTaxAmount"));
                receiptBillEntryVo.setIncomeAmount(resultSet.getString("FIncomeAmount"));
                receiptBillEntryVo.setOffSetMoneydefineID(resultSet.getString("FOffSetMoneydefineID"));
                receiptBillEntryVo.setMonth(resultSet.getString("FMonth"));
                receiptBillEntryVo.setRatio(resultSet.getString("FRatio"));
                receiptBillEntryVo.setLadderPriceAndValues(resultSet.getString("FLadderPriceAndValues"));
                receiptBillEntryVo.setInvoicedAmount(resultSet.getString("FInvoicedAmount"));
                receiptBillEntryVo.setInvoicedID(resultSet.getString("FInvoicedID"));
                receiptBillEntryVo.setInvoicedType(resultSet.getString("FInvoicedType"));
                receiptBillEntryVo.setDataSourceType(resultSet.getString("FDataSourceType"));
                receiptBillEntryVo.setIsVoucher(resultSet.getString("FIsVoucher"));
                receiptBillEntryVo.setVoucherID(resultSet.getString("FVoucherID"));
                receiptBillEntryVo.setReceivePeriod(resultSet.getString("FReceivePeriod"));
                receiptBillEntryVo.setSettlementTypeID(resultSet.getString("FSettlementTypeID"));
                receiptBillEntryVo.setReFundAmount(resultSet.getString("FReFundAmount"));
                receiptBillEntryVo.setReceiveDate(resultSet.getString("FReceiveDate"));
                receiptBillEntryVo.setLastDosage(resultSet.getString("FLastDosage"));
                receiptBillEntryVo.setCurrentDosage(resultSet.getString("FCurrentDosage"));
                receiptBillEntryVo.setRoomID(resultSet.getString("FRoomID"));
                receiptBillEntryVo.setReceiveStartDate(resultSet.getString("FReceiveStartDate"));
                receiptBillEntryVo.setReceiveEndDate(resultSet.getString("FReceiveEndDate"));
                receiptBillEntryVo.setReduPenaltyAmount(resultSet.getString("FReduPenaltyAmount"));
                receiptBillEntryVo.setParentEntryID(resultSet.getString("FParentEntryID"));
                receiptBillEntryVo.setRange(resultSet.getString("FRange"));
                receiptBillEntryVo.setInsEntryID(resultSet.getString("FInsEntryID"));
                receiptBillEntryVo.setArPenaltyAmount(resultSet.getString("FArPenaltyAmount"));
                receiptBillEntryVo.setStoreAmount(resultSet.getString("FStoreAmount"));
                receiptBillEntryVo.setStoreBalance(resultSet.getString("FStoreBalance"));
                receiptBillEntryVo.setProjectID(resultSet.getString("FProjectID"));
                receiptBillEntryVo.setBalanceID(resultSet.getString("FBalanceID"));
                receiptBillEntryVo.setIsLock(resultSet.getString("FIsLock"));
                receiptBillEntryVo.setBillNo(resultSet.getString("FBillNo"));
                receiptBillEntryVo.setRemark(resultSet.getString("FRemark"));
                receiptBillEntryVo.setIsReducePenalty(resultSet.getString("FIsReducePenalty"));
                receiptBillEntryVo.setReceiveCreator(resultSet.getString("FReceiveCreator"));
                receiptBillEntryVo.setFirstStage(resultSet.getString("FFirstStage"));
                receiptBillEntryVo.setIsInvoiced(resultSet.getString("FIsInvoiced"));
                receiptBillEntryVo.setUpdateTime(resultSet.getString("FUpdateTime"));

                ctx.collect(receiptBillEntryVo);//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }

    }

    //关闭数据库连接
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

