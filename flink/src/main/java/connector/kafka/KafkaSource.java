package connector.kafka;

/**
 * @Author: wangsen
 * @Date: 2020/5/28 12:54
 * @Description:
 **/
public class KafkaSource {
    private String topic;
    private String message;
    private Integer partition;
    private Long offset;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "KafkaSource{" +
                "topic='" + topic + '\'' +
                ", message='" + message + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                '}';
    }
}
