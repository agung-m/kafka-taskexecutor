package com.tritronik.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

/**
 * @author agung
 *
 */
public class RemoteTaskExecutor {
	private Logger logger = LoggerFactory.getLogger(RemoteTaskExecutor.class);
	
	private static final int SLEEP_INTERVAL = 100;
	
	private Properties props;
	private Set<RemoteData> unfinished;
	private Set<RemoteData> finished;
	private Producer<String, RemoteData> submitProducer;
	private Consumer<String, JsonNode> notifyConsumer;
	private ObjectMapper objectMapper;
	private TypeReference<RemoteData> remoteDataTypeRef;
	
	public RemoteTaskExecutor(Properties props) {
		this.props = props;
		
		unfinished = new HashSet<>();
		
		submitProducer = new KafkaProducer<>(props);
		notifyConsumer = new KafkaConsumer<>(props);
		notifyConsumer.subscribe(Arrays.asList(props.getProperty("kafka.notify.topic")));
		
		this.props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, RemoteData.class.getName());
		this.objectMapper = new ObjectMapper();
		this.remoteDataTypeRef = new TypeReference<RemoteData>() {};
		
		finished = new HashSet<>();
	}
	
	public Set<RemoteData> getUnfinished() {
		return unfinished;
	}

	public Set<RemoteData> waitAll(int timeout) {
		int start = 0;
		while ((unfinished.size() > 0) && (start <= timeout)){
			try {
				waitAny();
				Thread.sleep(SLEEP_INTERVAL);
				start += SLEEP_INTERVAL;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return finished;
	}
	
	public Set<RemoteData> waitAny() {
		Set<RemoteData> currentFinished = new HashSet<>();
		ConsumerRecords<String, JsonNode> records = notifyConsumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, JsonNode> record : records) {
        	RemoteData jobData = objectMapper.convertValue(record.value(), remoteDataTypeRef);
        	
        	//String finJob = record.value().id;
        	boolean isFromThisBatch = unfinished.remove(jobData);
        	
        	if (isFromThisBatch) {
        		currentFinished.add(jobData);
        	}
        	else {
        		logger.warn("Received a finished job from any other batch: " + jobData.id);
        	}
        	finished.add(jobData);
        }
        return currentFinished;
	}
	
	public void submit(List<String> inputs, boolean force) {
		//submitProducer = new KafkaProducer<>(props);
		if (!unfinished.isEmpty()) {
			if (!force) {
				logger.warn("Unfinished jobs exist! submission is canceled.");
				return;
			}
			else {
				finished.clear();
				unfinished.clear();
			}
		}
		
		String batchId = Uuid.randomUuid().toString();
		
		int i = 0;
		for (String s: inputs) {
			unfinished.add(new RemoteData(batchId + ":" + Integer.toString(++i), s));
		}
		
		for (RemoteData jobData: unfinished) {
			jobData.setSubmitTimeMs(System.currentTimeMillis());
			submitProducer.send(new ProducerRecord<String, RemoteData>(props.getProperty("kafka.task.topic"), 
				batchId, jobData));
		}
	}
	
	public void close() {
		notifyConsumer.close();
		submitProducer.close();
	}
	
}
