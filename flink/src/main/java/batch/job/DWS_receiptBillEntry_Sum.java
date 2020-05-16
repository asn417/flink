package batch.job;

import batch.dwd.DWD_ReceiptBillEntry_IPF;
import warehouse.dwd.entity.DWD_ReceiptBillEntryVo;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * @Author: wangsen
 * @Date: 2020/4/15 11:10
 * @Description:
 **/
public class DWS_receiptBillEntry_Sum {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<DWD_ReceiptBillEntryVo> dwd_receiptBillEntry = env.createInput(new DWD_ReceiptBillEntry_IPF());

        long count = dwd_receiptBillEntry.count();

        System.out.println("count:"+count);

//        dwd_receiptBillEntry.groupBy("receiverID","businessType","settlementTypeID","moneyDefineID","tranDate")
        dwd_receiptBillEntry.groupBy(new KeySelector<DWD_ReceiptBillEntryVo, String>() {
            @Override
            public String getKey(DWD_ReceiptBillEntryVo vo) throws Exception {
                return vo.getReceiverID()+vo.getBusinessType()+vo.getSettlementTypeID()+vo.getMoneyDefineID()+vo.getTranDate().split(" ")[0];
            }
        });

    }
    class AmountCount implements ReduceFunction<DWD_ReceiptBillEntryVo>{

        @Override
        public DWD_ReceiptBillEntryVo reduce(DWD_ReceiptBillEntryVo dwd_receiptBillEntryVo, DWD_ReceiptBillEntryVo t1) throws Exception {
            return null;
        }
    }
}
