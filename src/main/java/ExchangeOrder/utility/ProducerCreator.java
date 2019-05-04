package ExchangeOrder.utility;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class ProducerCreator {
    
	public static Producer<Long, String> createProducer() {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);


        return new KafkaProducer<Long, String>(props);

    }
}
