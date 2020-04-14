package batch;

import entity.dwd.DWD_ReceiptBillEntryVo;
import entity.ods.ODS_ReceiptBillEntryVo;
import entity.ods.ODS_ReceiptBillVo;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @Author: wangsen
 * @Date: 2020/4/14 16:16
 * @Description:
 **/
public class ReadFromHBase {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<ODS_ReceiptBillVo> transferBill = env.createInput(new ReceiptBillInputFormat());
        DataSource<ODS_ReceiptBillEntryVo> transferBillEntry = env.createInput(new ReceiptBillEntryInputFormat());

        long count = transferBillEntry.leftOuterJoin(transferBill).where("headID").equalTo("iD").with(
                new JoinFunction<ODS_ReceiptBillEntryVo, ODS_ReceiptBillVo, DWD_ReceiptBillEntryVo>() {
                    @Override
                    public DWD_ReceiptBillEntryVo join(ODS_ReceiptBillEntryVo receiptBillEntryVo, ODS_ReceiptBillVo receiptBillVo) throws Exception {
                        DWD_ReceiptBillEntryVo dwdReceiptBillEntryVo = new DWD_ReceiptBillEntryVo();

                        if (receiptBillEntryVo != null && receiptBillVo != null) {

                            dwdReceiptBillEntryVo.setiD(receiptBillEntryVo.getiD());
                            dwdReceiptBillEntryVo.setNumber(receiptBillEntryVo.getNumber());
                            dwdReceiptBillEntryVo.setHeadID(receiptBillEntryVo.getHeadID());
                            dwdReceiptBillEntryVo.setMoneyDefineID(receiptBillEntryVo.getMoneyDefineID());
                            dwdReceiptBillEntryVo.setMoneyStandardID(receiptBillEntryVo.getMoneyStandardID());
                            dwdReceiptBillEntryVo.setMoneyType(receiptBillEntryVo.getMoneyType());
                            dwdReceiptBillEntryVo.setPeriod(receiptBillEntryVo.getPeriod());
                            dwdReceiptBillEntryVo.setReceivableDate(receiptBillEntryVo.getReceivableDate());
                            dwdReceiptBillEntryVo.setReceivableAmount(receiptBillEntryVo.getReceivableAmount());
                            dwdReceiptBillEntryVo.setRevPenaltyAmount(receiptBillEntryVo.getRevPenaltyAmount());
                            dwdReceiptBillEntryVo.setRevAmount(receiptBillEntryVo.getRevAmount());
                            dwdReceiptBillEntryVo.setDescription(receiptBillEntryVo.getDescription());
                            dwdReceiptBillEntryVo.setBusinessType(receiptBillEntryVo.getBusinessType());
                            dwdReceiptBillEntryVo.setStatus(receiptBillEntryVo.getStatus());
                            dwdReceiptBillEntryVo.setCreator(receiptBillEntryVo.getCreator());
                            dwdReceiptBillEntryVo.setCreateTime(receiptBillEntryVo.getCreateTime());
                            dwdReceiptBillEntryVo.seteCID(receiptBillEntryVo.geteCID());
                            dwdReceiptBillEntryVo.setReceiveID(receiptBillEntryVo.getReceiveID());
                            dwdReceiptBillEntryVo.setQty(receiptBillEntryVo.getQty());
                            dwdReceiptBillEntryVo.setPrice(receiptBillEntryVo.getPrice());
                            dwdReceiptBillEntryVo.setDeleteTime(receiptBillEntryVo.getDeleteTime());
                            dwdReceiptBillEntryVo.setIsDelete(receiptBillEntryVo.getIsDelete());
                            dwdReceiptBillEntryVo.setTaxRate(receiptBillEntryVo.getTaxRate());
                            dwdReceiptBillEntryVo.setTaxAmount(receiptBillEntryVo.getTaxAmount());
                            dwdReceiptBillEntryVo.setIncomeAmount(receiptBillEntryVo.getIncomeAmount());
                            dwdReceiptBillEntryVo.setOffSetMoneydefineID(receiptBillEntryVo.getOffSetMoneydefineID());
                            dwdReceiptBillEntryVo.setMonth(receiptBillEntryVo.getMonth());
                            dwdReceiptBillEntryVo.setRatio(receiptBillEntryVo.getRatio());
                            dwdReceiptBillEntryVo.setLadderPriceAndValues(receiptBillEntryVo.getLadderPriceAndValues());
                            dwdReceiptBillEntryVo.setInvoicedAmount(receiptBillEntryVo.getInvoicedAmount());
                            dwdReceiptBillEntryVo.setInvoicedID(receiptBillEntryVo.getInvoicedID());
                            dwdReceiptBillEntryVo.setInvoicedType(receiptBillEntryVo.getInvoicedType());
                            dwdReceiptBillEntryVo.setDataSourceType(receiptBillEntryVo.getDataSourceType());
                            dwdReceiptBillEntryVo.setIsVoucher(receiptBillEntryVo.getIsVoucher());
                            dwdReceiptBillEntryVo.setVoucherID(receiptBillEntryVo.getVoucherID());
                            dwdReceiptBillEntryVo.setReceivePeriod(receiptBillEntryVo.getReceivePeriod());
                            dwdReceiptBillEntryVo.setSettlementTypeID(receiptBillEntryVo.getSettlementTypeID());
                            dwdReceiptBillEntryVo.setReFundAmount(receiptBillEntryVo.getReFundAmount());
                            dwdReceiptBillEntryVo.setReceiveDate(receiptBillEntryVo.getReceiveDate());
                            dwdReceiptBillEntryVo.setLastDosage(receiptBillEntryVo.getLastDosage());
                            dwdReceiptBillEntryVo.setCurrentDosage(receiptBillEntryVo.getCurrentDosage());
                            dwdReceiptBillEntryVo.setRoomID(receiptBillEntryVo.getRoomID());
                            dwdReceiptBillEntryVo.setReceiveStartDate(receiptBillEntryVo.getReceiveStartDate());
                            dwdReceiptBillEntryVo.setReceiveEndDate(receiptBillEntryVo.getReceiveEndDate());
                            dwdReceiptBillEntryVo.setReduPenaltyAmount(receiptBillEntryVo.getReduPenaltyAmount());
                            dwdReceiptBillEntryVo.setParentEntryID(receiptBillEntryVo.getParentEntryID());
                            dwdReceiptBillEntryVo.setRange(receiptBillEntryVo.getRange());
                            dwdReceiptBillEntryVo.setInsEntryID(receiptBillEntryVo.getInsEntryID());
                            dwdReceiptBillEntryVo.setArPenaltyAmount(receiptBillEntryVo.getArPenaltyAmount());
                            dwdReceiptBillEntryVo.setStoreAmount(receiptBillEntryVo.getStoreAmount());
                            dwdReceiptBillEntryVo.setStoreBalance(receiptBillEntryVo.getStoreBalance());
                            dwdReceiptBillEntryVo.setProjectID(receiptBillEntryVo.getProjectID());
                            dwdReceiptBillEntryVo.setBalanceID(receiptBillEntryVo.getBalanceID());
                            dwdReceiptBillEntryVo.setIsLock(receiptBillEntryVo.getIsLock());
                            dwdReceiptBillEntryVo.setBillNo(receiptBillEntryVo.getBillNo());
                            dwdReceiptBillEntryVo.setRemark(receiptBillEntryVo.getRemark());
                            dwdReceiptBillEntryVo.setIsReducePenalty(receiptBillEntryVo.getIsReducePenalty());
                            dwdReceiptBillEntryVo.setReceiveCreator(receiptBillEntryVo.getReceiveCreator());
                            dwdReceiptBillEntryVo.setFirstStage(receiptBillEntryVo.getFirstStage());
                            dwdReceiptBillEntryVo.setIsInvoiced(receiptBillEntryVo.getIsInvoiced());
                            dwdReceiptBillEntryVo.setUpdateTime(receiptBillEntryVo.getFUpdateTime());
                            dwdReceiptBillEntryVo.setOrgID(receiptBillVo.getOrgID());
                            dwdReceiptBillEntryVo.setCustomerID(receiptBillVo.getCustomerID());
                            dwdReceiptBillEntryVo.setChequeID(receiptBillVo.getChequeID());
                            dwdReceiptBillEntryVo.setChequeNumber(receiptBillVo.getChequeNumber());
                            dwdReceiptBillEntryVo.setTranDate(receiptBillVo.getTranDate());
                            dwdReceiptBillEntryVo.setReceiveID(receiptBillEntryVo.getReceiveID());
                            dwdReceiptBillEntryVo.setBillID(receiptBillVo.getBillID());
                            dwdReceiptBillEntryVo.setCancel(receiptBillVo.getCancel());
                            dwdReceiptBillEntryVo.setCancelTime(receiptBillVo.getCancelTime());
                            dwdReceiptBillEntryVo.setIsSms(receiptBillVo.getIsSms());
                            dwdReceiptBillEntryVo.setPrintCount(receiptBillVo.getPrintCount());
                            dwdReceiptBillEntryVo.setSource(receiptBillVo.getSource());
                            dwdReceiptBillEntryVo.setSourceState(receiptBillVo.getSourceState());
                            dwdReceiptBillEntryVo.setThirdPayBillID(receiptBillVo.getThirdPayBillID());
                            dwdReceiptBillEntryVo.setIsRefund(receiptBillVo.getIsRefund());
                            dwdReceiptBillEntryVo.setSettEntryName(receiptBillVo.getSettEntryName());
                            dwdReceiptBillEntryVo.setLockDescription(receiptBillVo.getLockDescription());
                            dwdReceiptBillEntryVo.setIsConfirm(receiptBillVo.getIsConfirm());
                            dwdReceiptBillEntryVo.setTradeNo(receiptBillVo.getTradeNo());
                            dwdReceiptBillEntryVo.setContributors(receiptBillVo.getContributors());
                            dwdReceiptBillEntryVo.setContributor(receiptBillVo.getContributor());
                            dwdReceiptBillEntryVo.setBillType(receiptBillVo.getBillType());
                            dwdReceiptBillEntryVo.setOverRevAmount(receiptBillVo.getOverRevAmount());
                            dwdReceiptBillEntryVo.setIdentifyPeople(receiptBillVo.getIdentifyPeople());
                            dwdReceiptBillEntryVo.setFileIDs(receiptBillVo.getFileIDs());
                            dwdReceiptBillEntryVo.setReceiptWriter(receiptBillVo.getReceiptWriter());
                            dwdReceiptBillEntryVo.setReceiptWriteTime(receiptBillVo.getReceiptWriteTime());
                            dwdReceiptBillEntryVo.setReceiptOperator(receiptBillVo.getReceiptOperator());
                            dwdReceiptBillEntryVo.setConfirmCancelDescription(receiptBillVo.getConfirmCancelDescription());
                            dwdReceiptBillEntryVo.setConfirmReceiptDate(receiptBillVo.getConfirmReceiptDate());
                        }
                        return dwdReceiptBillEntryVo;
                    }
                }
        ).count();
        System.out.println("=========count=======:"+count);
        env.execute();
    }
}
