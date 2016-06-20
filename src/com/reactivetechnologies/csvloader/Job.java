package com.reactivetechnologies.csvloader;

import java.io.Serializable;

public class Job implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private int jobIndex;
	Object jobDefn = null;
	private String payload;
	public Object getJobDefn(){
		return jobDefn;
	}
	public void setJobDefn(Object jobDefn){
		this.jobDefn = jobDefn;
	}
  public int getJobIndex() {
    return jobIndex;
  }
  public void setJobIndex(int jobIndex) {
    this.jobIndex = jobIndex;
  }
  public String getPayload() {
    return payload;
  }
  public void setPayload(String payload) {
    this.payload = payload;
  }
}
