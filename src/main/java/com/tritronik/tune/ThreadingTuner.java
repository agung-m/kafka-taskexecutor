package com.tritronik.tune;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadingTuner {
	final Logger logger = LoggerFactory.getLogger(ThreadingTuner.class);
	
	OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
	private int numCpus;
	
	public ThreadingTuner() {
		numCpus = Runtime.getRuntime().availableProcessors();
		logger.info("numCpus = " + numCpus + ", arch = " + osBean.getArch());
	}
	
	public int numIdleCpus() {
		double idleCpuUsage = 10.0 - osBean.getSystemLoadAverage();
		return (int) Math.floor(idleCpuUsage);
	}
	
}
