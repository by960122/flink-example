import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.tools.jline_embedded.internal.Nullable;

/**
 * Author:BYDylan
 * Date:2020/5/8
 * Description:实现 kaSerializationSchema
 */
public class KafkaSerializationSchemaDemo<T>
        implements org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema<T>, KafkaContextAware<T> {
    private String topic;

    public KafkaSerializationSchemaDemo(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp) {

        return new ProducerRecord<>(topic, element.toString().getBytes());
    }

    @Override
    public String getTargetTopic(T element) {
        return null;
    }
}
