package com.reactivetechnologies.csvloader;

import java.io.Serializable;

public class Job implements Serializable{
	
	private static final long serialVersionUID = 1L;
	int jobIndex;
	Object jobDefn = null;
	String payload;
	public Object getJobDefn(){
		return jobDefn;
	}
	public void setJobDefn(Object jobDefn){
		this.jobDefn = jobDefn;
	}
}
