package com.tritronik.kafka.test;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import com.tritronik.kafka.RemoteData;
import com.tritronik.kafka.RemoteTaskWorker;
import com.tritronik.kafka.WorkerFunction;

public class TaskWorkerTest {
	public static void main(String[] args) {
		Properties props = new Properties();
		try {
			InputStream inputStream = TaskWorkerTest.class.getClassLoader().getResourceAsStream("config.properties");
			props.load(inputStream);
		} 
		catch (Exception e) {
			 System.out.println("Sorry, unable to find config.properties");
             return;
		}
		
		WorkerFunction func = new WorkerFunction() {
			Random random = new Random();
			
			@Override
			public boolean apply(RemoteData data) {
				// Simulate processing time inside the worker
				int delay = 1000 + random.nextInt(5000);
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				data.setFinishTimeMs(System.currentTimeMillis());
				data.status = "finished";
				return true;
			}
		};
		
		RemoteTaskWorker worker = new RemoteTaskWorker(props);
		worker.setWorkerFunction(func);
		worker.listen();
		
	}
}
