package com.tritronik.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RemoteData {
	@JsonProperty
	public String id;
	
	@JsonProperty
	public String value;
	
	@JsonProperty
	public String submitTime;
	
	@JsonProperty
	public String finishTime;
	
	@JsonProperty
	public String status;
	
	private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public RemoteData() {
		this("");
	}
	
	public RemoteData(String id) {
		this(id, "", "", "");
	}
	
	public RemoteData(String id, String value) {
		this(id, value, "", "");
	}
	
	public RemoteData(String id, String value, String submitTime, String finishTime) {
		this.id = id;
		this.value = value;
		this.submitTime = submitTime;
		this.finishTime = finishTime;
		this.status = "";
	}
	
	public void setSubmitTimeMs(long ms) {
		submitTime = dateFormatter.format(new Date(ms));
	}
	
	public void setFinishTimeMs(long ms) {
		finishTime = dateFormatter.format(new Date(ms));
	}
	
	@Override
	public boolean equals(Object obj) {
		return (obj instanceof RemoteData) && ((RemoteData) obj).id.equals(this.id);
	}
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}
	
	@Override
	public String toString() {
		return "{id=" + id + ", value=" + value + ", submitTime=" + submitTime
				+ ", finishTime=" + finishTime + ", status=" + status + "}";
	}
	
}
