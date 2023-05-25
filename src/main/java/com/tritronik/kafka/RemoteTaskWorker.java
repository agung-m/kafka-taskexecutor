package com.tritronik.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tritronik.tune.ThreadingTuner;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

/**
 * @author agung
 *
 */

public class RemoteTaskWorker {
	final Logger logger = LoggerFactory.getLogger(RemoteTaskWorker.class);
	
	private WorkerFunction workerFunction;
	private Properties props;
	private int numThreads = 1;
	
	private Producer<String, RemoteData> notifyProducer;
	private Consumer<String, JsonNode> taskConsumer;
	private boolean stopped = false;
	
	private ObjectMapper objectMapper;
	private boolean enableThreadTuning;
	private ThreadingTuner threadingTuner;
	
	public RemoteTaskWorker(Properties props) {
		this.props = props;
		this.props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, RemoteData.class.getName());
		this.objectMapper = new ObjectMapper();
		
		String propNumThreads = props.getProperty("taskexecutor.worker.threads");
		if (propNumThreads != null) {
			numThreads = Integer.valueOf(propNumThreads); 
		}
		String propEnableThreadTuning = props.getProperty("taskexecutor.worker.threads.tuning.enabled");
		if (propEnableThreadTuning != null) {
			enableThreadTuning = Boolean.parseBoolean(propEnableThreadTuning); 
			if (enableThreadTuning) {
				threadingTuner = new ThreadingTuner();
			}
		}
	}
	
	public void setWorkerFunction(WorkerFunction workerFunction) {
		this.workerFunction = workerFunction;
	}

	public void shutdown() {
		stopped = true;
	}
	
	public void listen() {
		notifyProducer = new KafkaProducer<>(props);
		
		taskConsumer = new KafkaConsumer<>(props);
		taskConsumer.subscribe(Arrays.asList(props.getProperty("kafka.task.topic")));
		
		// Create task executor
		//ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
		int prevNumThreads = numThreads;
		
		logger.info("Worker is listening..");
		
		TypeReference<RemoteData> remoteDataTypeRef = new TypeReference<RemoteData>() {};
		
		while (!stopped) {
			//List<ConsumerRecord<String, Serializable>> failedRecords = new ArrayList<>();
			if (enableThreadTuning) {
				int numIdleCpus = threadingTuner.numIdleCpus();
				if ((numIdleCpus != prevNumThreads) && (numIdleCpus < numThreads)) {
					logger.warn("Re-adjusted the number of threads from " + prevNumThreads + " to " + numIdleCpus);
					executor.setCorePoolSize(numIdleCpus);
					prevNumThreads = numIdleCpus;
				}
			}
			
			ConsumerRecords<String, JsonNode> records = taskConsumer.poll(Duration.ofMillis(500));
	        for (ConsumerRecord<String, JsonNode> record : records) {
	        	RemoteData jobData = objectMapper.convertValue(record.value(), remoteDataTypeRef);
	        	logger.info("Received task " + jobData.id);
	        	Runnable workerThread = new WorkerThread(record.key(), jobData, workerFunction,
	        			notifyProducer, props.getProperty("kafka.notify.topic"));
	        	executor.execute(workerThread);
	        }
	        
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		taskConsumer.close();
		notifyProducer.close();
	}
	
}

class WorkerThread implements Runnable {
	final Logger logger = LoggerFactory.getLogger(WorkerThread.class);
	
	//private ConsumerRecord<String, RemoteData> record;
	private RemoteData jobData;
	private String batchId;
	private WorkerFunction func;
	private String destTopic;
	private Producer<String, RemoteData> producer;
	
	public WorkerThread(String batchId, RemoteData jobData, WorkerFunction func,
			Producer<String, RemoteData> producer, String destTopic) {
		//this.record = record;
		this.batchId = batchId;
		this.jobData = jobData;
		this.func = func;
		this.producer = producer;
		this.destTopic = destTopic;
	}

	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		
		boolean success = func.apply(jobData);
    	if (success) {
    		producer.send(new ProducerRecord<String, RemoteData>(destTopic, batchId, jobData));
    		//jobData = null;
    	}
    	else {
    		// To be implemented
    		logger.error("Worker function has returned an error state");
    	}
    	long elapsed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
    	logger.info("Finished job " + jobData.id + " (" + elapsed + " s)");
	}
	
}
