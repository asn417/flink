package batch.job;

import batch.ods.*;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import warehouse.ods.entity.*;

/**
 * @Author: wangsen
 * @Date: 2020/4/14 16:16
 * @Description: 读取ODS层相关维表和事实表数据，数据降维到dwd_ receiptBillEntry
 **/
public class ReadFromHBase {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        //房间维表
        DataSet<ODS_RoomVo> room = env.createInput(new ODS_Room_IPF());
        //收款员维表
        DataSet<ODS_ReceiptReceiverVo> receiver = env.createInput(new ODS_ReceiptReceiver_IPF());
        //款项维表
        DataSet<ODS_MoneyDefineVo> moneyDefine = env.createInput(new ODS_MoneyDefine_IPF());
        //项目维表
        DataSet<ODS_ProjectVo> project = env.createInput(new ODS_Project_IPF());
        //结算方式维表
        DataSet<ODS_SettlementTypeVo> settlementType = env.createInput(new ODS_SettlementType_IPF());

        //收款单事实表
        DataSet<ODS_ReceiptBillVo> transferBill = env.createInput(new ODS_ReceiptBill_IPF());
        //实收分录事实表
        DataSet<ODS_ReceiptBillEntryVo> transferBillEntry = env.createInput(new ODS_ReceiptBillEntry_IPF());


        /*select *
        from t_cc_receiptbillentry entry
        LEFT JOIN t_cc_receiptbill bill on bill.FID = entry.FHeadID and entry.FIsDelete = '0' and bill.FIsDelete = '0' and bill.FCancel = '0' and bill.FStatus = '1'
        LEFT JOIN t_bdc_project project on project.FID = entry.FProjectID
        LEFT JOIN t_cc_moneydefine moneyDefine on moneyDefine.FID = entry.FMoneyDefineID
        LEFT JOIN t_cc_receiptreceiver receiver on receiver.FEmployeeID = bill.FReceiverID
        LEFT JOIN t_pc_room room on room.FID = entry.FRoomID
        LEFT JOIN t_cc_settlementtype settle on settle.FID = entry.FSettlementTypeID and settle.FIsDelete = '0';*/

        long count = transferBillEntry/*.filter(new FilterFunction<ODS_ReceiptBillEntryVo>() {
            @Override
            public boolean filter(ODS_ReceiptBillEntryVo receiptBillEntryVo) throws Exception {
                return "0".equals(receiptBillEntryVo.getIsDelete());//过滤已删除的明细(每个表可以在连接前过滤(但可能会不准确)，也可以在连接后一起过滤(建议最后一起过滤))
            }
        })*/.leftOuterJoin(transferBill/*.filter(new FilterFunction<ODS_ReceiptBillVo>() {
            @Override
            public boolean filter(ODS_ReceiptBillVo receiptBillVo) throws Exception {//过滤已删除和无效的收款单
                return "0".equals(receiptBillVo.getIsDelete()) && "1".equals(receiptBillVo.getStatus());
            }
        })*/).where("headID").equalTo("iD").with(
                new JoinFunction<ODS_ReceiptBillEntryVo, ODS_ReceiptBillVo, DWD_ReceiptBillEntryVo>() {
                    @Override
                    public DWD_ReceiptBillEntryVo join(ODS_ReceiptBillEntryVo receiptBillEntryVo, ODS_ReceiptBillVo receiptBillVo) throws Exception {
                        DWD_ReceiptBillEntryVo dwdReceiptBillEntryVo = new DWD_ReceiptBillEntryVo();

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
                        dwdReceiptBillEntryVo.setBalanceID(receiptBillEntryVo.getBalanceID());
                        dwdReceiptBillEntryVo.setIsLock(receiptBillEntryVo.getIsLock());
                        dwdReceiptBillEntryVo.setBillNo(receiptBillEntryVo.getBillNo());
                        dwdReceiptBillEntryVo.setRemark(receiptBillEntryVo.getRemark());
                        dwdReceiptBillEntryVo.setIsReducePenalty(receiptBillEntryVo.getIsReducePenalty());
                        dwdReceiptBillEntryVo.setReceiveCreator(receiptBillEntryVo.getReceiveCreator());
                        dwdReceiptBillEntryVo.setFirstStage(receiptBillEntryVo.getFirstStage());
                        dwdReceiptBillEntryVo.setIsInvoiced(receiptBillEntryVo.getIsInvoiced());
                        dwdReceiptBillEntryVo.setUpdateTime(receiptBillEntryVo.getUpdateTime());
                        //左连接，右表可能不匹配，则为null
                        if (receiptBillVo != null) {
                            dwdReceiptBillEntryVo.setOrgID(receiptBillVo.getOrgID());
                            if (receiptBillVo.getProjectID() == null){
                                dwdReceiptBillEntryVo.setProjectID("aaa");
                            }else {
                                dwdReceiptBillEntryVo.setProjectID(receiptBillVo.getProjectID());
                            }
                            dwdReceiptBillEntryVo.setCustomerID(receiptBillVo.getCustomerID());
                            dwdReceiptBillEntryVo.setChequeID(receiptBillVo.getChequeID());
                            dwdReceiptBillEntryVo.setChequeNumber(receiptBillVo.getChequeNumber());
                            dwdReceiptBillEntryVo.setTranDate(receiptBillVo.getTranDate());
                            dwdReceiptBillEntryVo.setReceiverID(receiptBillVo.getReceiverID());
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
                            dwdReceiptBillEntryVo.setBillIsDelete(receiptBillVo.getIsDelete());
                            dwdReceiptBillEntryVo.setBillDeleteTime(receiptBillVo.getDeleteTime());
                            dwdReceiptBillEntryVo.setBillStatus(receiptBillVo.getStatus());
                        }
                        return dwdReceiptBillEntryVo;
                    }
                })
                .leftOuterJoin(project).where("projectID").equalTo("iD").with(new JoinFunction<DWD_ReceiptBillEntryVo, ODS_ProjectVo, DWD_ReceiptBillEntryVo>() {
                    @Override
                    public DWD_ReceiptBillEntryVo join(DWD_ReceiptBillEntryVo receiptBillEntryVo, ODS_ProjectVo projectVo) throws Exception {
                        if (projectVo != null) {//leftjoin有可能出现空指针
                            receiptBillEntryVo.setProjectName(projectVo.getName());
                        }
                        return receiptBillEntryVo;
                    }
                })
                /*.leftOuterJoin(moneyDefine).where("moneyDefineID").equalTo("iD").with(new JoinFunction<DWD_ReceiptBillEntryVo, ODS_MoneyDefineVo, DWD_ReceiptBillEntryVo>() {
                    @Override
                    public DWD_ReceiptBillEntryVo join(DWD_ReceiptBillEntryVo receiptBillEntryVo, ODS_MoneyDefineVo moneyDefineVo) throws Exception {
                        if (moneyDefineVo != null) {
                            receiptBillEntryVo.setMoneyDefineName(moneyDefineVo.getName());
                        }
                        return receiptBillEntryVo;
                    }
                })
                .leftOuterJoin(receiver).where("receiverID").equalTo("employeeID").with(new JoinFunction<DWD_ReceiptBillEntryVo, ODS_ReceiptReceiverVo, DWD_ReceiptBillEntryVo>() {
                    @Override
                    public DWD_ReceiptBillEntryVo join(DWD_ReceiptBillEntryVo dwd_receiptBillEntryVo, ODS_ReceiptReceiverVo ods_receiptReceiverVo) throws Exception {
                        if (ods_receiptReceiverVo != null) {
                            dwd_receiptBillEntryVo.setReceiverName(ods_receiptReceiverVo.getEmployeeName());
                        }
                        return dwd_receiptBillEntryVo;
                    }
                })
                .leftOuterJoin(room).where("roomID").equalTo("iD").with(new JoinFunction<DWD_ReceiptBillEntryVo, ODS_RoomVo, DWD_ReceiptBillEntryVo>() {

                    @Override
                    public DWD_ReceiptBillEntryVo join(DWD_ReceiptBillEntryVo dwd_receiptBillEntryVo, ODS_RoomVo roomVo) throws Exception {
                        if (roomVo != null) {
                            dwd_receiptBillEntryVo.setRoomName(roomVo.getName());
                        }
                        return dwd_receiptBillEntryVo;
                    }
                })
                .leftOuterJoin(settlementType).where("settlementTypeID").equalTo("iD").with(new JoinFunction<DWD_ReceiptBillEntryVo, ODS_SettlementTypeVo, DWD_ReceiptBillEntryVo>() {
                    @Override
                    public DWD_ReceiptBillEntryVo join(DWD_ReceiptBillEntryVo dwd_receiptBillEntryVo, ODS_SettlementTypeVo ods_settlementTypeVo) throws Exception {
                        if (ods_settlementTypeVo != null) {
                            dwd_receiptBillEntryVo.setSettlementTypeName(ods_settlementTypeVo.getName());
                        }
                        return dwd_receiptBillEntryVo;
                    }
                })*/
                .filter(new FilterFunction<DWD_ReceiptBillEntryVo>() {
                    @Override
                    public boolean filter(DWD_ReceiptBillEntryVo dwd_receiptBillEntryVo) throws Exception {
                        return "0".equals(dwd_receiptBillEntryVo.getIsDelete()) && "0".equals(dwd_receiptBillEntryVo.getBillIsDelete()) && "1".equals(dwd_receiptBillEntryVo.getBillStatus());
                    }
                }).count();

        System.out.println("=====================================count:" + count);

        //filter.output(new DWD_ReceiptBillEntry_OPF());
        env.execute();
    }
}
