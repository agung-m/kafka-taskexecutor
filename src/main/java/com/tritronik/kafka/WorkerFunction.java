package com.tritronik.kafka;

public interface WorkerFunction {
	boolean apply(RemoteData data);
}
