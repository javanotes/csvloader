package com.reactivetechnologies.csvloader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConfigLoader {
	
  private static final Logger log = Logger.getLogger(ConfigLoader.class.getSimpleName());
	public static final String CONFIG_FILE = "config.properties";
	
	public static final String LOAD_SEPARATOR = "LOAD_SEPARATOR"; 
	public static final String LOAD_BATCH_SIZE = "LOAD_BATCH_SIZE";
	public static final String LOAD_PER_THREAD = "LOAD_PER_THREAD";
	public static final String LOAD_FILE_NAME = "LOAD_FILE_NAME";
	public static final String LOAD_SQL_TEMPLATE = "LOAD_SQL_TEMPLATE";
	public static final String LOAD_IGNORE_FIRST_LINE = "LOAD_IGNORE_FIRST_LINE";
	public static final String INSERT_INTO_TABLE = "INSERT_INTO_TABLE";
	public static final String SELECT_FROM_TABLE = "SELECT_FROM_TABLE";
	public static final String TARGET_DATABASE = "TARGET_DATABASE";
	public static final String SOURCE_DATABASE = "SOURCE_DATABASE";
	
	public static final String CPOOL_DATASOURCE = "CPOOL_DATASOURCE";
	
	public static final String MYSQL = "MYSQL";
	public static final String ORACLE = "ORACLE";
	public static final String MSSQL = "MSSQL";
	public static final String PGSQL = "PGSQL";
	
	public static final String MYSQL_DB_URL = "MYSQL_DB_URL";
	public static final String MYSQL_DB_DRIVER_CLASS = "MYSQL_DB_DRIVER_CLASS";
	public static final String MYSQL_DB_USERNAME = "MYSQL_DB_USERNAME";
	public static final String MYSQL_DB_PASSWORD = "MYSQL_DB_PASSWORD";
	public static final String ORACLE_DB_URL = "ORACLE_DB_URL";
	public static final String ORACLE_DB_DRIVER_CLASS = "MYSQL_DB_DRIVER_CLASS";
	public static final String ORACLE_DB_USERNAME = "ORACLE_DB_USERNAME";
	public static final String ORACLE_DB_PASSWORD = "ORACLE_DB_PASSWORD";
	public static final String MSSQL_DB_USERNAME = "MSSQL_DB_USERNAME";
	public static final String MSSQL_DB_DRIVER_CLASS = "MYSQL_DB_DRIVER_CLASS";
	public static final String MSSQL_DB_PASSWORD = "MSSQL_DB_PASSWORD";
	public static final String MSSQL_DB_URL = "MSSQL_DB_URL";
	public static final String PGSQL_DB_DS = "PGSQL_DB_DS";
	public static final String PGSQL_DB_DRIVER_CLASS = "MYSQL_DB_DRIVER_CLASS";
	public static final String PGSQL_DB_URL = "PGSQL_DB_URL";
	public static final String PGSQL_DB_USERNAME = "PGSQL_DB_USERNAME";
	public static final String PGSQL_DB_PASSWORD = "PGSQL_DB_PASSWORD";
	
	public static final String[] ISO_8601_DATE_FORMATS = {"yyyy-mm-dd HH:mm","yyyy-mm-dd HH:mm:ss", "yyyy-mm-dd HH:mmZ", "yyyy-mm-dd HH:mm:ssZ", "yyyy-mm-dd'T'HH:mm",
	    "yyyy-mm-dd'T'HH:mmZ", "yyyy-mm-dd'T'HH:mm:ss", "yyyy-mm-dd'T'HH:mm:ssZ", "yyyy-mm-dd", "yyyy-mm-ddZ", "yyyy-mm", "yyyy"};
  public static final String COMMIT_ON_BATCH_FAIL = "COMMIT_ON_BATCH_FAIL";
  
  public static final String SYS_PROP_INORDER = "proc.inorder";
  //public static final String SYS_PROP_INORDER_QSIZE = "proc.inorder.qsize";
  public static final String SYS_PROP_MMAP_IO = "mem.mapped";
  public static final String SYS_PROP_MMAP_SIZE = "mmap.size";
  public static final String SYS_PROP_BUFF_IO = "buff.io";
  public static final String SYS_PROP_THREADS = "max.thread";
  public static final String SYS_PROP_SKIP_BLK_FLD = "skip.blank.field";
  public static final String SYS_PROP_SKIP_INV_FLD = "skip.invalid.field";
  public static final String SYS_PROP_DATE_FMT = "date.formats";

	public static boolean isImmediateProcessing()
	{
	  return System.getProperty(ConfigLoader.SYS_PROP_INORDER) != null;
	}
	public static int getBatchSize()
	{
	  return Integer.valueOf(ConfigLoader.getConfig().getProperty(ConfigLoader.LOAD_BATCH_SIZE, "100"));
	}
	
	private static Properties config = null;
	
	public static Properties getConfig() {
        FileInputStream fis = null;
        if(config == null){
        	config = new Properties();
        	String cfgFile = System.getProperty(CONFIG_FILE, CONFIG_FILE);
        	try {
	            fis = new FileInputStream(cfgFile);
	            config.load(fis);
	            fis.close();
	            verifyConfig();
	        } catch (IOException e) {
	          try {
	            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(cfgFile);
	            if(is == null)
	              throw new FileNotFoundException(cfgFile);
              config.load(is);
              verifyConfig();
            } catch (IOException e1) {
              throw new ExceptionInInitializerError(e1);
            }
	        }
        }
		return config;
	}
	
	public static boolean verifyConfig(){
        if(config != null){
    		try{
    			int loadPerThread = Integer.parseInt(ConfigLoader.getConfig().getProperty(ConfigLoader.LOAD_PER_THREAD));
    			int ignoreFirstLine = Integer.parseInt(ConfigLoader.getConfig().getProperty(ConfigLoader.LOAD_IGNORE_FIRST_LINE));
    			
    			if(ignoreFirstLine < 0 || ignoreFirstLine > 1){
    			  log.severe("ERROR: Permissible values for "+ConfigLoader.LOAD_IGNORE_FIRST_LINE+" is 0 or 1 only.");
    				return false;
    			}
    			if(loadPerThread <= 0 ){
    				log.severe("ERROR: "+ConfigLoader.LOAD_PER_THREAD+" should be a numeric value greater than 0.");
    				return false;
    			}
    			if(ConfigLoader.getConfig().getProperty(ConfigLoader.LOAD_FILE_NAME) == null){
    			  log.severe("ERROR: "+ConfigLoader.LOAD_FILE_NAME+" is not specified.");
    				return false;				
    			}
    			if(ConfigLoader.getConfig().getProperty(ConfigLoader.LOAD_SEPARATOR) == null){
    			  log.severe("ERROR: "+ConfigLoader.LOAD_SEPARATOR+" is not specified.");
    				return false;				
    			}
     			if(ConfigLoader.getConfig().getProperty(ConfigLoader.INSERT_INTO_TABLE) == null){    				
     			 log.severe("ERROR: "+ConfigLoader.INSERT_INTO_TABLE+" is not specified.");
    				return false;				
    			}
     			String target = ConfigLoader.getConfig().getProperty(ConfigLoader.TARGET_DATABASE);
     			if(target == null){
     			 log.severe("ERROR: "+ConfigLoader.TARGET_DATABASE+" is not specified.");
    				return false;				
    			}
     			else{
     	    		if( !target.equals(ConfigLoader.MYSQL) && !target.equals(ConfigLoader.MSSQL) 
     	    			&& !target.equals(ConfigLoader.PGSQL) && !target.equals(ConfigLoader.ORACLE)){
     	    		 log.severe("ERROR: Invalid Target in properties file");
     	    			return false;
     	    		}
     	    		else{
     	    			if( target.equals(ConfigLoader.MYSQL) 
     	    					&& (ConfigLoader.getConfig().getProperty(ConfigLoader.MYSQL_DB_URL) == null 
     	    					|| ConfigLoader.getConfig().getProperty(ConfigLoader.MYSQL_DB_USERNAME) == null
     	    					|| ConfigLoader.getConfig().getProperty(ConfigLoader.MYSQL_DB_PASSWORD) == null)){
     	    			 log.severe("ERROR: All required MYSQL parameters are not provided.");
     	     	    			return false;
     	     	    	}
     	    			if( target.equals(ConfigLoader.ORACLE) 
     	    					&& (ConfigLoader.getConfig().getProperty(ConfigLoader.ORACLE_DB_URL) == null 
     	    					|| ConfigLoader.getConfig().getProperty(ConfigLoader.ORACLE_DB_USERNAME) == null
     	    					|| ConfigLoader.getConfig().getProperty(ConfigLoader.ORACLE_DB_PASSWORD) == null)){
     	    			 log.severe("ERROR: All required ORACLE parameters are not provided.");
     	     	    			return false;
     	     	    	}
     	    			if( target.equals(ConfigLoader.MSSQL) 
     	    					&& (ConfigLoader.getConfig().getProperty(ConfigLoader.MSSQL_DB_USERNAME) == null 
     	    					|| ConfigLoader.getConfig().getProperty(ConfigLoader.MSSQL_DB_PASSWORD) == null
     	    					|| ConfigLoader.getConfig().getProperty(ConfigLoader.MSSQL_DB_URL) == null)){
     	    			 log.severe("ERROR: All required MSSQL parameters are not provided.");
     	     	    			return false;
     	     	    	}
     	    			if( target.equals(ConfigLoader.PGSQL) 
     	    					&& (ConfigLoader.getConfig().getProperty(ConfigLoader.PGSQL_DB_DS) == null 
     	    					|| ConfigLoader.getConfig().getProperty(ConfigLoader.PGSQL_DB_URL) == null
     	     	    			|| ConfigLoader.getConfig().getProperty(ConfigLoader.PGSQL_DB_USERNAME) == null
     	     	    			|| ConfigLoader.getConfig().getProperty(ConfigLoader.PGSQL_DB_PASSWORD) == null)){
     	    			 log.warning("All required PGSQL parameters are not provided.");
     	     	    			return false;
     	     	    	}
    	    		}
     			}
    		}catch(Exception e){
    			e.printStackTrace();
    			log.log(Level.SEVERE, "", e);
    		}
        }

		return true;
	}
}
