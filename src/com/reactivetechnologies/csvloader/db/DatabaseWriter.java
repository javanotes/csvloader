package com.reactivetechnologies.csvloader.db;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.reactivetechnologies.csvloader.ConfigLoader;
import com.reactivetechnologies.csvloader.Job;
import com.reactivetechnologies.csvloader.JobExecutor;

public class DatabaseWriter implements JobExecutor, Closeable{
	private static final Logger log = Logger.getLogger(DatabaseWriter.class.getSimpleName());
	private ArrayList<Job> jobList = new ArrayList<Job>();
	private final int jobCapacity;
	private StringBuilder sqlTemplate = null;
	private final AtomicLong counter;
	private final DataSource ds;
	/**
	 * 
	 * @param jobCapacity
	 * @param counter
	 */
	public DatabaseWriter(int jobCapacity, AtomicLong counter, DataSource ds){
		this.jobCapacity = jobCapacity;
		this.counter = counter;
    jobQ = ConfigLoader.isImmediateProcessing()
        ? new ArrayBlockingQueue<Job>(this.jobCapacity)
        : new ArrayBlockingQueue<Job>(ConfigLoader.getBatchSize());
		this.ds = ds;
	}
	private BlockingQueue<Job> jobQ;
	public void execute(){
		Thread t = new Thread(this);
		t.start();
	}

	private void prepareSQLTemplate(Job job){
		if(sqlTemplate == null){
			String dbTable = ConfigLoader.getConfig().getProperty(ConfigLoader.INSERT_INTO_TABLE);
			if(dbTable != null){
				sqlTemplate = new StringBuilder("INSERT INTO "+dbTable+" VALUES (");
			}
			String[] values = (String[]) job.getJobDefn();
      for(int i=0; i<values.length; i++){
        sqlTemplate.append("?");
        if(i < values.length-1){
          sqlTemplate.append(",");
        }
      }
      sqlTemplate.append(")");
		}
	}
	
	private DatabaseSession prepareSession(Job job) throws SQLException
	{
	  DatabaseSession session = new DatabaseSession(counter, ds);  
    try {
      session.setBatchSize(ConfigLoader.getBatchSize());
    } catch (NumberFormatException e) {
      
    }
    prepareSQLTemplate(job);
    session.prepareStatement(sqlTemplate.toString());
    return session;
	}
	
	/**
	 * @deprecated
	 */
  void _run()
	{
    DatabaseSession session = new DatabaseSession(counter, ds); 
    try {
      session.setBatchSize(ConfigLoader.getBatchSize());
    } catch (NumberFormatException e) {
      
    }
    prepareSQLTemplate(jobList.get(0));
    try 
    {
      session.prepareStatement(sqlTemplate.toString());
      
      try {
        jobQ.poll(1, TimeUnit.SECONDS);
      } catch (InterruptedException e1) {
        
      }
      for(Job job : jobList){
        String[] values = (String[]) job.getJobDefn();
        try {
          session.addBatch(values, job.getJobIndex());
          //log.info("Record# "+job.jobIndex+"> "+job.payload);
        } catch (Exception e) {
          log.warning("["+Thread.currentThread().getName()+"] Skipping load record ["+job.getPayload()+"] "+e.getMessage());
        }
      }
      session.executeBatch();
      
    } 
    catch (SQLException e) {
      log.log(Level.SEVERE, "Failed to run job executor", e);
    }
    finally
    {
      session.close();
    }
  
	}
  private DatabaseSession session = null;
  /**
   * 
   */
	private void run0()
	{
	      
    try 
    {
      
      Job job;
      while(true)
      {
        try 
        {
          job = jobQ.poll(10, TimeUnit.MILLISECONDS);
          if(job instanceof PoisonPill)
            break;
          if(job != null)
          {
            if(session == null)
            {
              session = prepareSession(job);
              log.info("Prepared session..");
            }
            String[] values = (String[]) job.getJobDefn();
            try 
            {
              session.addBatch(values, job.getJobIndex());
              //log.info("Record# "+job.jobIndex+"> "+job.payload);
            } catch (Exception e) {
              log.warning("Batch exception at [Rec#"+job.getJobIndex()+"] Skipping load record ["+job.getPayload()+"] "+e.getMessage());
            }
          }
        } catch (InterruptedException e1) {
          
        }
      }
    } catch (SQLException e2) {
      log.log(Level.SEVERE, "Unable to get DB session", e2);
    } 
    finally
    {
      close();
    }
     
	}
	
	
	@Override
	public void run() {
	  run0();
	}

	private final AtomicInteger offered = new AtomicInteger(0);
	/**
	 * 
	 */
	private static class PoisonPill extends Job
	{

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
	  
	}
	/**
	 * 
	 */
	public void stop()
	{
	  log.info("stopping db writer..");
	  offer(new PoisonPill());
	}
	private void offer(Job j)
	{
	  if (!stopped) {
      try {
        while (!jobQ.offer(j, 10, TimeUnit.MILLISECONDS))
          ;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } 
    }
	}
	/**
	 * 
	 */
	public boolean addJob(Job job) {
	  if(offered.getAndIncrement() < jobCapacity)
	  {
	    offer(job);
	    return true;
	  }
	  stop();
	  return false;
		
	}
	/**
	 * 
	 */
  @Override
  public boolean addJobImmediate(Job job) {
    if (!stopped) {
      return jobQ.offer(job);
    }
    return false;
  }

  private volatile boolean stopped;
  @Override
  public void close()  {
    if(session != null)
    {
      try {
        session.executeBatch();
      } catch (SQLException e) {
        log.log(Level.SEVERE, "Execute batch caught exception on closing", e);
      }
      session.close();
      session = null;
    }
    if (jobQ != null) {
      jobQ.clear();
      jobQ = null;
    }
    stopped = true;
  }
}
