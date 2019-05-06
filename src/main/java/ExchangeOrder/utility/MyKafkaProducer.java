package ExchangeOrder.utility;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ExchangeOrder.model.LogResult;
import ExchangeOrder.model.OrderLogBack;


public class MyKafkaProducer {
	
	private static Producer<Long, String> producer;
	public static ObjectMapper mapper;
	static {
		producer = ProducerCreator.createProducer();
		mapper = new ObjectMapper();
	}
	
	public static void produceMessage(LogResult<OrderLogBack> result) throws InterruptedException, ExecutionException, JsonProcessingException {
		
        ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(Constant.TOPIC_NAME,mapper.writeValueAsString(result));
        RecordMetadata metadata = producer.send(record).get();
        System.out.println("Record sent with key " + result.getResult().getGlobalMatchingEngineLogId() + " to partition " + metadata.partition()
        + " with offset " + metadata.offset());
	}
}
