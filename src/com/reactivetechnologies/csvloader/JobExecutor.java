package com.reactivetechnologies.csvloader;

public interface JobExecutor extends Runnable{
		
	public void execute();
	public boolean addJob(Job job);
	public boolean addJobImmediate(Job job);
	public void stop();
}
