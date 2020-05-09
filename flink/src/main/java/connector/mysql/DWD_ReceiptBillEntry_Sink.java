package connector.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author: wangsen
 * @Date: 2020/5/8 21:53
 * @Description:
 **/
public class DWD_ReceiptBillEntry_Sink extends RichSinkFunction<DWD_ReceiptBillEntryVo> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName("com.mysql.jdbc.Driver");
        // 获取数据库连接
        connection = DriverManager.getConnection("jdbc:mysql://172.20.181.91/owner_cloud", "wjctest", "CrdWj@201702");//获取连接
        preparedStatement = connection.prepareStatement("insert into dwd_receiptBillEntry " +
                "values (?,?,?,?,?,?,?)");//insert sql在配置文件中
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(DWD_ReceiptBillEntryVo value, Context context) throws Exception {
        try {
            preparedStatement.setString(1,value.getiD());
            preparedStatement.setString(2,value.getHeadID());
            preparedStatement.setString(3,value.getProjectName());
            preparedStatement.setString(4,value.getReceiverName());
            preparedStatement.setString(5,value.getMoneyDefineName());
            preparedStatement.setString(6,value.getRoomName());
            preparedStatement.setString(7,value.getSettlementTypeName());
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
