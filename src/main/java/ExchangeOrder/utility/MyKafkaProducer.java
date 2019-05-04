package ExchangeOrder.utility;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import ExchangeOrder.model.OrderLogBack;

public class MyKafkaProducer {
	
	private static Producer<Long, String> producer;
	static {
		producer = ProducerCreator.createProducer();
	}
	
	public static void produceMessage(OrderLogBack olb) throws InterruptedException, ExecutionException {
        ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(Constant.TOPIC_NAME,
        		"This is record " + olb.getGlobalMatchingEngineLogId());
        RecordMetadata metadata = producer.send(record).get();
        System.out.println("Record sent with key " + olb.getGlobalMatchingEngineLogId() + " to partition " + metadata.partition()
        + " with offset " + metadata.offset());
	}
}
