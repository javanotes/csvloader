package com.reactivetechnologies.csvloader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.reactivetechnologies.csvloader.db.DataSourceFactory;
import com.reactivetechnologies.csvloader.db.DatabaseWriter;
import com.reactivetechnologies.csvloader.io.AsciiFileReader;
import com.reactivetechnologies.csvloader.net.SocketCSVLoader;

public class CSVLoader implements JobAllocator {
  
  static
  {
    System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tr %3$s %4$s:  %5$s%6$s %n");
  }
  private static final Logger log = Logger.getLogger(CSVLoader.class.getSimpleName());
	private ExecutorService threadPool;
	private Job job = null;
	protected JobExecutor executor = null;
	private int loadPerThread = 0;
	private final AtomicLong loadCount = new AtomicLong();
	private int threadCount = 0, executorCount = 1;
	private DataSource ds;
	private boolean immediate;
	/**
	 * 
	 * @param loadPerThread
	 */
	public CSVLoader(int loadPerThread)
	{
	  ds = DataSourceFactory.getDataSource();
		this.loadPerThread = loadPerThread;
		immediate = ConfigLoader.isImmediateProcessing();
		if(System.getProperty(ConfigLoader.SYS_PROP_THREADS) != null)
		{
		  try 
		  {
        int noOfThreads = Integer.valueOf(System.getProperty(ConfigLoader.SYS_PROP_THREADS));
        threadPool = Executors.newFixedThreadPool(noOfThreads, new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "jobExecutor-"+(threadCount++));
            t.setDaemon(true);
            return t;
          }
        });
      } catch (NumberFormatException e) {}
		}
		if(threadPool == null)
		{
		  threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r, "jobExecutor-"+(threadCount++));
          t.setDaemon(true);
          return t;
        }
      });
		}
		
		executor = new DatabaseWriter(loadPerThread, loadCount, ds);
		threadPool.execute(executor);
	}
	
	@Override
  public int getloadCount(){
		return loadCount.intValue();
	}

	private List<JobExecutor> executors;
	
	/**
	 * Order of records may not be maintained
	 */
	private void allocateImmediate()
  {
	  if(executors == null)
	  {
	    synchronized (this) {
        if (executors == null) {
          executors = new ArrayList<>();
          if (executor != null) {
            executors.add(executor);
          } 
        }
      }
	  }
    if(job != null)
    {
      boolean added = false;
      do 
      {
        for (JobExecutor exec : executors) {
          if (exec.addJobImmediate(job)) {
            added = true;
            job = null;
            break;
          }
        }
        if (!added) {
          executor = new DatabaseWriter(loadPerThread, loadCount, ds);
          threadPool.execute(executor);
          executors.add(executor);
          executorCount++;
        } 
      } while (!added);
      
    }
    else
    { 
      for(JobExecutor exec : executors)
      {
        exec.stop();
      }
      
    }
  }
	/**
	 * 
	 */
	private void allocateInOrder()
	{
	  if(job != null)
    {
      if(executor.addJob(job)){
        job = null;
      }
      else
      {
        //pool.execute(executor);
        executor = new DatabaseWriter(loadPerThread, loadCount, ds);
        threadPool.execute(executor);
        executorCount++;
        allocateInOrder();
      }
    }
    else
    { 
      executor.stop();
    }
	}
	/**
	 * 
	 */
	public void allocate(){
		if(!immediate)
		  allocateInOrder();
		else
		  allocateImmediate();
	}

	@Override
  public void clean() throws Exception{
		threadPool.shutdown();
		threadPool.awaitTermination(600, TimeUnit.MINUTES);
	}
	
	private void loadByChannelIO(String fileName, int ignoreFirstLine, String separator)
	{
	  try(AsciiFileReader reader = new AsciiFileReader(new File(fileName), System.getProperty(ConfigLoader.SYS_PROP_MMAP_IO) != null))
	  {
	    String strLine = "";
      while( (strLine = reader.readLine()) != null){
        if(ignoreFirstLine == 1){
          ignoreFirstLine = 0;
          continue;
        }
        loadNextLine(strLine, separator);
      }
      allocate();
	  }
	  catch (IOException e) {
     log.log(Level.SEVERE, "File reading error", e);
  }}
	private int line = 1;
	
	/**
	 * Loads next line
	 * @param strLine
	 * @param separator
	 */
	protected void loadNextLine(String strLine, String separator)
	{
	  //log.info(strLine);
	  String[] values = strLine.split(separator, -1);
    job = new Job();
    job.setJobIndex(line++);
    job.setJobDefn(values);
    job.setPayload(strLine);
    allocate();
    
	}
	
  private void loadByBufferedIO(String fileName, int ignoreFirstLine, String separator)
	{
	  try 
	  {
      BufferedReader br = new BufferedReader( new FileReader(fileName));
            String strLine = "";
            while( (strLine = br.readLine()) != null){
              if(ignoreFirstLine == 1){
                ignoreFirstLine = 0;
                continue;
              }
              //System.out.println((idx++)+"=> "+strLine);
              loadNextLine(strLine, separator);
            }
            allocate();
            br.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
  protected long startTime;
  /**
   * 
   * @param fileName
   * @param ignoreFirstLine
   * @param separator
   */
	
  protected void load(String fileName, int ignoreFirstLine, String separator){
    startTime = System.currentTimeMillis();
	  if(System.getProperty(ConfigLoader.SYS_PROP_BUFF_IO) != null)
	    loadByBufferedIO(fileName, ignoreFirstLine, separator);
	  else
	    loadByChannelIO(fileName, ignoreFirstLine, separator);
	}
  /**
   * 
   * @param args
   */
	public static void run(String...args)
  {
    try 
    {
      int loadPerThread = Integer.parseInt(
          ConfigLoader.getConfig().getProperty(ConfigLoader.LOAD_PER_THREAD));
      
      CSVLoader loader = null;
      try {
        int port = Integer.parseInt(args[0]);
        loader = new SocketCSVLoader(loadPerThread, port, ConfigLoader.getConfig()
            .getProperty(ConfigLoader.LOAD_SEPARATOR), Integer.parseInt(ConfigLoader.getConfig()
                .getProperty(ConfigLoader.LOAD_IGNORE_FIRST_LINE)));
      } catch (NumberFormatException e) {
        loader = new CSVLoader(loadPerThread);
      }
            
      log.info("############ Start execution ############");
      loader.startTime = System.currentTimeMillis();
      loader.load();
      loader.clean();
      long end = System.currentTimeMillis();
      log.info("############ End execution ##############");
      
      log.info("Loaded " + loader.getloadCount() + " records using "
          + loader.executorCount + " executor(s), on "+loader.threadCount+" thread(s) in "+timeString(end-loader.startTime));
      
    } 
    catch (Exception e) {
      e.printStackTrace();
    }
  }
	private static String timeString(long duration)
	{
	  StringBuilder s = new StringBuilder();
	  long div = TimeUnit.MILLISECONDS.toMinutes(duration);
	  s.append(div).append(" min ");
	  duration -= TimeUnit.MINUTES.toMillis(div);
	  div = TimeUnit.MILLISECONDS.toSeconds(duration);
	  s.append(div).append(" sec ");
	  duration -= TimeUnit.SECONDS.toMillis(div);
	  s.append(duration).append(" ms ");
    return s.toString();
	  
	}
	public static void main(String[] args){
		
		run(args);		
				
	}

  @Override
  public void load() {
    
    int ignoreFirstLine = Integer.parseInt(ConfigLoader.getConfig()
        .getProperty(ConfigLoader.LOAD_IGNORE_FIRST_LINE));
    String loadFileName = ConfigLoader.getConfig()
        .getProperty(ConfigLoader.LOAD_FILE_NAME);
    String sep = ConfigLoader.getConfig()
        .getProperty(ConfigLoader.LOAD_SEPARATOR);
    
    load(loadFileName, ignoreFirstLine, sep);
    
  }

}
