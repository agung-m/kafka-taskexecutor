package com.tritronik.kafka.test;

import java.io.InputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.tritronik.kafka.RemoteData;
import com.tritronik.kafka.RemoteTaskExecutor;


/**
 * @author Mulya Agung
 *
 */

public class TaskExecutorTest {
	public static void main(String[] args) {
		// The number of simulated inputs
		int filenameIdMax = 10;
		
		Properties props = new Properties();
		try {
			InputStream inputStream = TaskExecutorTest.class.getClassLoader().getResourceAsStream("config.properties");
			props.load(inputStream);
			//filenameIdMax = Integer.valueOf(props.getProperty("filename.id.max"));
		} 
		catch (Exception e) {
			 System.out.println("Sorry, unable to find config.properties");
             return;
		}
		
		List<String> filenames = new ArrayList<>();
		for (int i = 1; i < filenameIdMax+1; i++) {
			filenames.add("file-" + i);
		}
		
		RemoteTaskExecutor task = new RemoteTaskExecutor(props);
		task.submit(filenames, false);
		
		while (!task.getUnfinished().isEmpty()) {
			Set<RemoteData> finished = task.waitAny();
			for (RemoteData rd: finished)
				System.out.println("- Finish " + rd.toString());
		}
		task.close();
		System.out.println("-- All finished.");
		
	}
}
