package com.reactivetechnologies.csvloader;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class DataSourceFactory {

  static final String MYSQL_DS_CLASS = "com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource";
  static final String ORACLE_DS_CLASS = "oracle.jdbc.pool.OracleDataSource";
  static final String MSSQL_DS_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDataSource";
  static final String PGSQL_DS_CLASS = "org.postgresql.ds.PGPoolingDataSource";
  
  static final String PROP = "PROP";
  static final String SETTER = "_PROP_SET_";
	private static Properties props = null;
	private static final Logger log = Logger.getLogger(DataSourceFactory.class.getSimpleName());
	private static class CPoolMeta
	{
	  public CPoolMeta(DataSource ds) {
      super();
      this.ds = ds;
    }
    final DataSource ds;
	  String urlProp,usrProp,pwdProp,drvProp;
	  String url,usr,pwd,drv;
	  Map<String, String> custProps = new HashMap<>();
	}
	
	private static CPoolMeta getCPool()
	{
	  CPoolMeta meta = null;
	  try 
    {
      DataSource ds = (DataSource) Class.forName(props.getProperty(ConfigLoader.CPOOL_DATASOURCE))
          .newInstance();
      
      meta = new CPoolMeta(ds);
      for(Entry<Object, Object> entry : props.entrySet())
      {
        String key = entry.getKey().toString();
        String val = entry.getValue().toString();
        
        if(key.startsWith(ConfigLoader.CPOOL_DATASOURCE))
        {
          if(key.equals(ConfigLoader.CPOOL_DATASOURCE))
            continue;
          
          if(key.contains("URL")){
            meta.urlProp = val;
          }
          else if(key.contains("PWD")){
            meta.pwdProp = val;
          }
          else if(key.contains("USR")){
            meta.usrProp = val;
          }
          else if(key.contains("DRV")){
            meta.drvProp = val;
          }
          else
          {
            try {
              String prop = key.substring((ConfigLoader.CPOOL_DATASOURCE+SETTER).length(), key.length());
              meta.custProps.put(prop, val);
            } catch (Exception e) {
              // ignore
            }
          }
        }
      }
    } catch (Exception e) {
      log.warning("Exception on loading custom connection pooling library "
          + "{ "+e.getMessage()+" }");
      e.printStackTrace();
    }
    return meta;

	}
	/**
	 * 
	 * @return
	 */
  public static DataSource getDataSource() 
  {
      DataSource ds = null;
    	if(props == null){
    		props = ConfigLoader.getConfig();
    	}
    	if(props.getProperty(ConfigLoader.CPOOL_DATASOURCE) != null)
    	{
    	  CPoolMeta cpool = getCPool();
    	  if(cpool != null)
    	  {
    	    getDriverDSImpl(cpool);
    	    try {
            ds = initializeCPool(cpool);
            log.info("Using cpool datasource => "+ds.getClass().getName());
          } catch (Exception e) {
            log.warning("Exception on setting connection pool property. "
                + " { "+e.getMessage()+" }");
          }
    	  }
    	}
    	if(ds == null){
    	  ds = getDriverDSImpl(null);
    	  log.info("Using driver datasource => "+ds.getClass().getName());
    	}
    	
      return ds;
    }
    
  private static String setter(String prop)
  {
    return "set"+String.valueOf(prop.charAt(0)).toUpperCase()+prop.substring(1);
  }
  private static DataSource initializeCPool(CPoolMeta cpool) throws Exception {
    
    //mandatory properties for a cpool impl
    propertySetter(cpool.ds, setter(cpool.pwdProp), cpool.pwd);
    propertySetter(cpool.ds, setter(cpool.usrProp), cpool.usr);
    propertySetter(cpool.ds, setter(cpool.urlProp), cpool.url);
    propertySetter(cpool.ds, setter(cpool.drvProp), cpool.drv);
    
    for(Entry<String, String> e : cpool.custProps.entrySet())
    {
      String k = e.getKey();
      String v = e.getValue();
      
      try {
        propertySetterInt(cpool.ds, setter(k), Integer.valueOf(v));
      } catch (Exception e1) {
        try {
          propertySetterLong(cpool.ds, setter(k), Long.valueOf(v));
        } catch (Exception e2) {
          try {
            propertySetterDouble(cpool.ds, setter(k), Double.valueOf(v));
          } catch (Exception e3) {
            try {
              propertySetter(cpool.ds, setter(k), v);
            } catch (Exception e4) {
              propertySetterBool(cpool.ds, setter(k), Boolean.parseBoolean(v));
            }
          }
        }
      }
    }
    return cpool.ds;
  }
  private static DataSource getDriverDSImpl(CPoolMeta cpool)
  {
    String target = props.getProperty(ConfigLoader.TARGET_DATABASE);
    switch(target){
      case ConfigLoader.MYSQL:
        return getMySQLDataSource(cpool);
      case ConfigLoader.MSSQL:
        return getSQLServerDataSource(cpool);
      case ConfigLoader.PGSQL:
        return getPostGreSQLDataSource(cpool);
      case ConfigLoader.ORACLE:
        return getOracleDataSource(cpool);
      default:
        throw new UnsupportedOperationException("ERROR: Invalid Target in properties file");
    }
  }
    private static DataSource getMySQLDataSource(CPoolMeta cpool) {
      try 
      {
        if(cpool != null)
        {
          cpool.pwd = props.getProperty(ConfigLoader.MYSQL_DB_PASSWORD);
          cpool.usr = props.getProperty(ConfigLoader.MYSQL_DB_USERNAME);
          cpool.url = props.getProperty(ConfigLoader.MYSQL_DB_URL);
          cpool.drv = props.getProperty(ConfigLoader.MYSQL_DB_DRIVER_CLASS);
          return null;
        }
        
        Object mysqlDS = Class.forName(MYSQL_DS_CLASS)
            .newInstance();
        propertySetter(mysqlDS, "setURL",
            props.getProperty(ConfigLoader.MYSQL_DB_URL));
        propertySetter(mysqlDS, "setUser",
            props.getProperty(ConfigLoader.MYSQL_DB_USERNAME));
        propertySetter(mysqlDS, "setPassword",
            props.getProperty(ConfigLoader.MYSQL_DB_PASSWORD));
        
        
        return (DataSource) mysqlDS;
        
      } catch (Exception e) {
        
        UnsupportedOperationException ue = new UnsupportedOperationException(
            ConfigLoader.MYSQL);
        ue.initCause(e);
        throw ue;
      }
      
    }
    private static void propertySetter(Object obj, String method, String prop) throws Exception
    {
      Method m = obj.getClass().getMethod(method, String.class);
      if(m != null)
      {
        m.setAccessible(true);
        m.invoke(obj, prop);
      }
    }
    private static void propertySetterBool(Object obj, String method, Boolean prop) throws Exception
    {
      Method m;
      try 
      {
        m = obj.getClass().getMethod(method, Boolean.class);
        if(m != null)
        {
          m.setAccessible(true);
          m.invoke(obj, prop);
          return;
        }
        
      } catch (Exception e) {
        
      }
      m = obj.getClass().getMethod(method, Boolean.TYPE);
      if(m != null)
      {
        m.setAccessible(true);
        m.invoke(obj, prop);
      }
    }
    private static void propertySetterInt(Object obj, String method, Integer prop) throws Exception
    {
      Method m;
      try 
      {
        m = obj.getClass().getMethod(method, Integer.class);
        if(m != null)
        {
          m.setAccessible(true);
          m.invoke(obj, prop);
          return;
        }
        
      } catch (Exception e) {
        
      }
      m = obj.getClass().getMethod(method, Integer.TYPE);
      if(m != null)
      {
        m.setAccessible(true);
        m.invoke(obj, prop);
      }
    }
    private static void propertySetterLong(Object obj, String method, Long prop) throws Exception
    {
      Method m;
      try 
      {
        m = obj.getClass().getMethod(method, Long.class);
        if(m != null)
        {
          m.setAccessible(true);
          m.invoke(obj, prop);
          return;
        }
        
      } catch (Exception e) {
        
      }
      m = obj.getClass().getMethod(method, Long.TYPE);
      if(m != null)
      {
        m.setAccessible(true);
        m.invoke(obj, prop);
      }
    }
    private static void propertySetterDouble(Object obj, String method, Double prop) throws Exception
    {
      Method m;
      try 
      {
        m = obj.getClass().getMethod(method, Double.class);
        if(m != null)
        {
          m.setAccessible(true);
          m.invoke(obj, prop);
          return;
        }
        
      } catch (Exception e) {
        
      }
      m = obj.getClass().getMethod(method, Double.TYPE);
      if(m != null)
      {
        m.setAccessible(true);
        m.invoke(obj, prop);
      }
    }

  private static DataSource getOracleDataSource(CPoolMeta cpool) {
    try 
    {
      
      if(cpool != null)
      {
        cpool.pwd = props.getProperty(ConfigLoader.ORACLE_DB_PASSWORD);
        cpool.usr = props.getProperty(ConfigLoader.ORACLE_DB_USERNAME);
        cpool.url = props.getProperty(ConfigLoader.ORACLE_DB_URL);
        cpool.drv = props.getProperty(ConfigLoader.ORACLE_DB_DRIVER_CLASS);
        return null;
      }
      
      Object oracleDS = Class.forName(ORACLE_DS_CLASS)
          .newInstance();
      propertySetter(oracleDS, "setURL",
          props.getProperty(ConfigLoader.ORACLE_DB_URL));
      propertySetter(oracleDS, "setUser",
          props.getProperty(ConfigLoader.ORACLE_DB_USERNAME));
      propertySetter(oracleDS, "setPassword",
          props.getProperty(ConfigLoader.ORACLE_DB_PASSWORD));
      
      
      
      return (DataSource) oracleDS;
      
    } catch (Exception e) {
      UnsupportedOperationException ue = new UnsupportedOperationException(
          ConfigLoader.ORACLE);
      ue.initCause(e);
      throw ue;
    }
    
  }

    private static DataSource getSQLServerDataSource(CPoolMeta cpool){
      try {
        
        if(cpool != null)
        {
          cpool.pwd = props.getProperty(ConfigLoader.MSSQL_DB_PASSWORD);
          cpool.usr = props.getProperty(ConfigLoader.MSSQL_DB_USERNAME);
          cpool.url = props.getProperty(ConfigLoader.MSSQL_DB_URL);
          cpool.drv = props.getProperty(ConfigLoader.MSSQL_DB_DRIVER_CLASS);
          return null;
        }
        
        Object sqlServerDS = Class.forName(MSSQL_DS_CLASS)
            .newInstance();
        propertySetter(sqlServerDS, "setURL",
            props.getProperty(ConfigLoader.MSSQL_DB_URL));
        propertySetter(sqlServerDS, "setUser",
            props.getProperty(ConfigLoader.MSSQL_DB_USERNAME));
        propertySetter(sqlServerDS, "setPassword",
            props.getProperty(ConfigLoader.MSSQL_DB_PASSWORD));
        
                
        return (DataSource) sqlServerDS;
        
      } catch (Exception e) {
        UnsupportedOperationException ue = new UnsupportedOperationException(
            ConfigLoader.MSSQL);
        ue.initCause(e);
        throw ue;
      }
    	
    }

    private static DataSource getPostGreSQLDataSource(CPoolMeta cpool){
      try 
      {
        if(cpool != null)
        {
          cpool.pwd = props.getProperty(ConfigLoader.PGSQL_DB_PASSWORD);
          cpool.usr = props.getProperty(ConfigLoader.PGSQL_DB_USERNAME);
          cpool.url = props.getProperty(ConfigLoader.PGSQL_DB_URL);
          cpool.drv = props.getProperty(ConfigLoader.PGSQL_DB_DRIVER_CLASS);
          return null;
        }
        
        Object postGreSQLDS = Class.forName(PGSQL_DS_CLASS)
            .newInstance();
        propertySetter(postGreSQLDS, "setUrl",
            props.getProperty(ConfigLoader.PGSQL_DB_URL));
        propertySetter(postGreSQLDS, "setUser",
            props.getProperty(ConfigLoader.PGSQL_DB_USERNAME));
        propertySetter(postGreSQLDS, "setPassword",
            props.getProperty(ConfigLoader.PGSQL_DB_PASSWORD));
        propertySetterInt(postGreSQLDS, "setMaxConnections", 10);
        propertySetter(postGreSQLDS, "setDataSourceName", ConfigLoader.PGSQL_DB_DS);
        
        
        
        return (DataSource) postGreSQLDS;
        
      } catch (Exception e) {
        UnsupportedOperationException ue = new UnsupportedOperationException(
            ConfigLoader.PGSQL);
        ue.initCause(e);
        throw ue;
      }
    	
    }
}