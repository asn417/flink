import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * @Author: wangsen
 * @Date: 2020/4/7 16:34
 * @Description: 反序列化binlog日志成message格式。
 **/
public class MyCustomDeserializationSchema extends AbstractDeserializationSchema<Message> {
    @Override
    public Message deserialize(byte[] bytes) throws IOException {
        Message message = CanalMessageDeserializer.deserializer(bytes);
        return message;
    }
}
