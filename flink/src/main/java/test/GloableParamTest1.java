package test;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author: wangsen
 * @Date: 2020/6/9 10:24
 * @Description: 生成上不同环境配置不同，需要能够动态传参
 **/
public class GloableParamTest1 {
    private static final Logger logger = LoggerFactory.getLogger(GloableParamTest1.class);

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment.fromElements("hello world");

        //获取传入的参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //注册给环境变量
        environment.getConfig().setGlobalJobParameters(parameterTool);
        //获取注册的配置
        ExecutionConfig.GlobalJobParameters parameters = environment.getConfig().getGlobalJobParameters();

        Map<String, String> map = parameters.toMap();
        //获取apollo.meta，并注册给系统变量
        String apollo = map.get("apollo");
        System.setProperty("apollo.meta", apollo);
        //apollo客户端就可以获取apollo中的配置了
        Config apolloConfig = ConfigService.getConfig("ECName_splittable");
        String key = apolloConfig.getProperty("receiptBill", "default value");
        logger.error("================key:"+key);

        for (Map.Entry<String,String> entry:map.entrySet()){
            logger.error("================key:{},value:{}================"+entry.getKey(),entry.getValue());
            System.out.println("================key:{"+entry.getKey()+"},value:{"+entry.getValue()+"}================");
            System.out.println("==========receiptBill:"+key);
        }

        streamSource.print("=========print========");
        environment.execute("param job");

    }
}
