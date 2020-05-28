package warehouse.dwd.source;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author: wangsen
 * @Date: 2020/5/8 21:12
 * @Description:
 **/
public class DWD_ReceiptBillEntry_Source extends RichSourceFunction<DWD_ReceiptBillEntryVo> {
    private static final Logger logger = LoggerFactory.getLogger(DWD_ReceiptBillEntry_Source.class);

    private Connection connection = null;
    private PreparedStatement ps = null;

    private String splitTableName = null;

    public DWD_ReceiptBillEntry_Source(String splitTableName){
        this.splitTableName = splitTableName;
    }

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");//加载数据库驱动
        connection = DriverManager.getConnection("jdbc:mysql://172.20.181.91/owner_cloud", "wjctest", "CrdWj@201702");//获取连接
        ps = connection.prepareStatement(
                "select DISTINCT entry.FID iD,entry.FNumber number,entry.FHeadID headID,entry.FMoneyDefineID moneyDefineID,entry.FMoneyStandardID moneyStandardID,entry.FMoneyType moneyType," +
                "entry.FPeriod period,entry.FReceivableDate receivableDate,entry.FReceivableAmount receivableAmount,entry.FRevPenaltyAmount revPenaltyAmount,entry.FRevAmount revAmount," +
                "entry.FDescription description,bill.FBusinessType businessType,entry.FStatus status,entry.FCreator creator,entry.FCreateTime createTime,entry.FECID eCID,entry.FReceiveID receiveID," +
                "entry.FQty qty,entry.FPrice price,entry.FTaxRate taxRate,entry.FTaxAmount taxAmount,entry.FIncomeAmount incomeAmount,entry.FOffSetMoneydefineID offSetMoneydefineID,entry.FMonth `month`," +
                "entry.FRatio ratio,entry.FLadderPriceAndValues ladderPriceAndValues,entry.FInvoicedAmount invoicedAmount,entry.FInvoicedID invoicedID,entry.FInvoicedType invoicedType,entry.FDataSourceType dataSourceType," +
                "entry.FIsVoucher isVoucher,entry.FVoucherID voucherID,entry.FReceivePeriod receivePeriod,entry.FSettlementTypeID settlementTypeID,entry.FReFundAmount reFundAmount,entry.FReceiveDate receiveDate," +
                "entry.FLastDosage lastDosage,entry.FCurrentDosage currentDosage,entry.FRoomID roomID,entry.FReceiveStartDate receiveStartDate,entry.FReceiveEndDate receiveEndDate,entry.FReduPenaltyAmount reduPenaltyAmount," +
                "entry.FParentEntryID parentEntryID,entry.FRange `range`,entry.FInsEntryID insEntryID,entry.FArPenaltyAmount arPenaltyAmount,entry.FStoreAmount storeAmount,entry.FStoreBalance storeBalance," +
                "entry.FProjectID projectID,entry.FBalanceID balanceID,entry.FIsLock isLock,entry.FBillNo billNo,entry.FRemark remark,entry.FIsReducePenalty isReducePenalty,entry.FReceiveCreator receiveCreator," +
                "entry.FFirstStage firstStage,entry.FIsInvoiced isInvoiced,entry.FUpdateTime updateTime," +
                "bill.FOrgID orgID,bill.FCustomerID customerID,bill.FChequeID chequeID,bill.FChequeNumber chequeNumber,SUBSTR(bill.FTranDate,1,10) tranDate,bill.FReceiverID receiverID,bill.FBillID billID," +
                "bill.FCancel cancel,bill.FCancelTime cancelTime,bill.FIsSms isSms,bill.FPrintCount printCount,bill.FSource source,bill.FSourceState sourceState," +
                "project.Fname projectName," +
                "receiver.FEmployeeName receiverName," +
                "moneyDefine.FName moneyDefineName," +
                "room.FName roomName,room.FProperty roomProperty," +
                "settle.FName settlementTypeName \n" +
                "from t_cc_receiptbillentry"+splitTableName+" entry \n" +
                "LEFT JOIN t_cc_receiptbill"+splitTableName+" bill on bill.FID = entry.FHeadID \n" +
                "LEFT JOIN t_bdc_project project on project.FID = entry.FProjectID \n" +
                "LEFT JOIN t_cc_moneydefine moneyDefine on moneyDefine.FID = entry.FMoneyDefineID \n" +
                "LEFT JOIN t_cc_receiptreceiver receiver on receiver.FEmployeeID = bill.FReceiverID and receiver.FProjectID = entry.FProjectID \n" +
                "LEFT JOIN t_pc_room room on room.FID = entry.FRoomID \n" +
                "LEFT JOIN t_cc_settlementtype settle on settle.FID = entry.FSettlementTypeID \n" +
                "where entry.FIsDelete = '0' and bill.FIsDelete = '0' and bill.FCancel = '0' and bill.FStatus = '1' and settle.FIsDelete = '0';");
    }

    @Override
    public void run(SourceContext<DWD_ReceiptBillEntryVo> ctx) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();

            while (resultSet.next()) {
                DWD_ReceiptBillEntryVo receiptBillEntryVo = new DWD_ReceiptBillEntryVo();

                receiptBillEntryVo.setiD(resultSet.getString("iD"));
                receiptBillEntryVo.setNumber(resultSet.getString("number"));
                receiptBillEntryVo.setHeadID(resultSet.getString("headID"));
                receiptBillEntryVo.setMoneyDefineID(resultSet.getString("moneyDefineID"));
                receiptBillEntryVo.setMoneyStandardID(resultSet.getString("moneyStandardID"));
                receiptBillEntryVo.setMoneyType(resultSet.getString("moneyType"));
                receiptBillEntryVo.setPeriod(resultSet.getString("period"));
                receiptBillEntryVo.setReceivableDate(resultSet.getString("receivableDate"));
                receiptBillEntryVo.setReceivableAmount(resultSet.getString("receivableAmount"));
                receiptBillEntryVo.setRevPenaltyAmount(StringUtils.isEmpty(resultSet.getString("revPenaltyAmount"))?BigDecimal.ZERO:new BigDecimal(resultSet.getString("revPenaltyAmount")));
                receiptBillEntryVo.setRevAmount(StringUtils.isEmpty(resultSet.getString("revAmount"))?BigDecimal.ZERO:new BigDecimal(resultSet.getString("revAmount")));
                receiptBillEntryVo.setDescription(resultSet.getString("description"));
                receiptBillEntryVo.setBusinessType(resultSet.getString("businessType"));
                receiptBillEntryVo.setStatus(resultSet.getString("status"));
                receiptBillEntryVo.setCreator(resultSet.getString("creator"));
                receiptBillEntryVo.setCreateTime(resultSet.getString("createTime"));
                receiptBillEntryVo.seteCID(resultSet.getString("eCID"));
                receiptBillEntryVo.setReceiveID(resultSet.getString("receiveID"));
                receiptBillEntryVo.setQty(resultSet.getString("qty"));
                receiptBillEntryVo.setPrice(resultSet.getString("price"));
                receiptBillEntryVo.setTaxRate(resultSet.getString("taxRate"));
                receiptBillEntryVo.setTaxAmount(resultSet.getString("taxAmount"));
                receiptBillEntryVo.setIncomeAmount(resultSet.getString("incomeAmount"));
                receiptBillEntryVo.setOffSetMoneydefineID(resultSet.getString("offSetMoneydefineID"));
                receiptBillEntryVo.setMonth(resultSet.getString("month"));
                receiptBillEntryVo.setRatio(resultSet.getString("ratio"));
                receiptBillEntryVo.setLadderPriceAndValues(resultSet.getString("ladderPriceAndValues"));
                receiptBillEntryVo.setInvoicedAmount(resultSet.getString("invoicedAmount"));
                receiptBillEntryVo.setInvoicedID(resultSet.getString("invoicedID"));
                receiptBillEntryVo.setInvoicedType(resultSet.getString("invoicedType"));
                receiptBillEntryVo.setDataSourceType(resultSet.getString("dataSourceType"));
                receiptBillEntryVo.setIsVoucher(resultSet.getString("isVoucher"));
                receiptBillEntryVo.setVoucherID(resultSet.getString("voucherID"));
                receiptBillEntryVo.setReceivePeriod(resultSet.getString("receivePeriod"));
                receiptBillEntryVo.setSettlementTypeID(resultSet.getString("settlementTypeID"));
                receiptBillEntryVo.setReFundAmount(resultSet.getString("reFundAmount"));
                receiptBillEntryVo.setReceiveDate(resultSet.getString("receiveDate"));
                receiptBillEntryVo.setLastDosage(resultSet.getString("lastDosage"));
                receiptBillEntryVo.setCurrentDosage(resultSet.getString("currentDosage"));
                receiptBillEntryVo.setRoomID(resultSet.getString("roomID"));
                receiptBillEntryVo.setReceiveStartDate(resultSet.getString("receiveStartDate"));
                receiptBillEntryVo.setReceiveEndDate(resultSet.getString("receiveEndDate"));
                receiptBillEntryVo.setReduPenaltyAmount(resultSet.getString("reduPenaltyAmount"));
                receiptBillEntryVo.setParentEntryID(resultSet.getString("parentEntryID"));
                receiptBillEntryVo.setRange(resultSet.getString("range"));
                receiptBillEntryVo.setInsEntryID(resultSet.getString("insEntryID"));
                receiptBillEntryVo.setArPenaltyAmount(resultSet.getString("arPenaltyAmount"));
                receiptBillEntryVo.setStoreAmount(resultSet.getString("storeAmount"));
                receiptBillEntryVo.setStoreBalance(resultSet.getString("storeBalance"));
                receiptBillEntryVo.setProjectID(resultSet.getString("projectID"));
                receiptBillEntryVo.setBalanceID(resultSet.getString("balanceID"));
                receiptBillEntryVo.setIsLock(resultSet.getString("isLock"));
                receiptBillEntryVo.setBillNo(resultSet.getString("billNo"));
                receiptBillEntryVo.setRemark(resultSet.getString("remark"));
                receiptBillEntryVo.setIsReducePenalty(resultSet.getString("isReducePenalty"));
                receiptBillEntryVo.setReceiveCreator(resultSet.getString("receiveCreator"));
                receiptBillEntryVo.setFirstStage(resultSet.getString("firstStage"));
                receiptBillEntryVo.setIsInvoiced(resultSet.getString("isInvoiced"));
                receiptBillEntryVo.setUpdateTime(resultSet.getString("updateTime"));


                receiptBillEntryVo.setOrgID(resultSet.getString("orgID"));
                receiptBillEntryVo.setCustomerID(resultSet.getString("customerID"));
                receiptBillEntryVo.setChequeID(resultSet.getString("chequeID"));
                receiptBillEntryVo.setChequeNumber(resultSet.getString("chequeNumber"));
                receiptBillEntryVo.setTranDate(resultSet.getString("tranDate"));
                receiptBillEntryVo.setReceiverID(resultSet.getString("receiverID"));
                receiptBillEntryVo.setBillID(resultSet.getString("billID"));
                receiptBillEntryVo.setCancel(resultSet.getString("cancel"));
                receiptBillEntryVo.setCancelTime(resultSet.getString("cancelTime"));
                receiptBillEntryVo.setIsSms(resultSet.getString("isSms"));
                receiptBillEntryVo.setPrintCount(resultSet.getString("printCount"));
                receiptBillEntryVo.setSource(resultSet.getString("source"));
                receiptBillEntryVo.setSourceState(resultSet.getString("sourceState"));

                receiptBillEntryVo.setProjectName(resultSet.getString("projectName"));
                receiptBillEntryVo.setReceiverName(resultSet.getString("receiverName"));
                receiptBillEntryVo.setMoneyDefineName(resultSet.getString("moneyDefineName"));
                receiptBillEntryVo.setRoomName(resultSet.getString("roomName"));
                receiptBillEntryVo.setRoomProperty(resultSet.getString("roomProperty"));
                receiptBillEntryVo.setSettlementTypeName(resultSet.getString("settlementTypeName"));

                ctx.collect(receiptBillEntryVo);//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
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
