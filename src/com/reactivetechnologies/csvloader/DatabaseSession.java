package com.reactivetechnologies.csvloader;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class DatabaseSession {
	
  private static final Logger log = Logger.getLogger(DatabaseSession.class.getSimpleName());
	//final DataSource dSource;
	private Connection conn = null;
	private PreparedStatement insertPstmt = null;
	private Statement batchStmt;
	Statement stmt = null;
	ResultSet rs = null;
	private int batchSize = 100;
		
	private final AtomicLong counter;
	/**
	 * 
	 * @param counter
	 */
	public DatabaseSession(AtomicLong counter, DataSource dSource)
	{
		try 
		{
			conn = dSource.getConnection();
			if(!conn.getMetaData().supportsBatchUpdates())
      {
        throw new UnsupportedOperationException("Batch execution not supported in JDBC driver");
      }
			this.counter = counter;
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	private void prepareType(ResultSet result, String dbTable) throws SQLException
	{
	  result.close();
    result = conn.getMetaData().getPrimaryKeys(null, null, dbTable);
    while(result.next())
    {
      primaryKeys.put(result.getShort(5), result.getString(4));
    }
    result.close();
    
    result = conn.getMetaData().getColumns(null, null, dbTable, null);
    
    reflectDataTypes(result);
    
    log.fine("-- Database metadata loaded --");
    log.fine(dataTypes.toString());
	}
	private void readMetadata(String dbTable) throws SQLException
	{
	  ResultSet result = conn.getMetaData().getTables(null, null, dbTable, null);
    try 
    {
      if (result.next()) 
      {
        prepareType(result, dbTable);
        
      } 
      else 
      {
        result.close();
        result = conn.getMetaData().getTables(null, null, dbTable.toLowerCase(), null);
        if (result.next()) 
        {
          prepareType(result, dbTable.toLowerCase());
          
        }
        else
        {
          result.close();
          result = conn.getMetaData().getTables(null, null, dbTable.toUpperCase(), null);
          if (result.next()) 
          {
            prepareType(result, dbTable.toUpperCase());
            
          }
          else
          {
            throw new IllegalArgumentException(
                "Table: " + dbTable + " not found");
          }
        }
        
      } 
    } finally {
      result.close();
    }
	}
	private String insertSQL, deleteSQL;
	public void prepareStatement(String sql) throws SQLException{
		try
		{
		  
			if(sql != null)
			{
				String dbTable = ConfigLoader.getConfig().getProperty(ConfigLoader.INSERT_INTO_TABLE);
				
				readMetadata(dbTable);
				
				String sql1 = sql.substring(0, sql.indexOf(dbTable) + dbTable.length());
				String sql2 = sql.substring(sql1.length());
				sql1 += "(";
				for(ColumnMeta col : dataTypes.values())
				{
				  sql1 += col.name + ",";
				}
				if(sql1.endsWith(","))
				{
				  sql1 = sql1.substring(0, sql1.length()-1);
				}
				sql1 += ")";
				insertSQL = sql1 + sql2;
				insertPstmt = conn.prepareStatement(insertSQL);
				log.fine(insertSQL);
				
				String and = "AND ";
				if(!primaryKeys.isEmpty() && !autoIncrement)
				{
				  log.fine("This table has non AI primary key. Will prepare delete statement");
				  String del = "DELETE FROM "+dbTable+" WHERE ";
				  for(String pkCol : primaryKeys.values())
				  {
				    del += pkCol + "=? "+and;
				  }
				  if(del.endsWith(and))
				  {
				    deleteSQL = del.substring(0, del.lastIndexOf(and));
	          log.info(deleteSQL);
				  }
				  				  
				}
				batchStmt = conn.createStatement();
        conn.setAutoCommit(false);
        log.info("["+Thread.currentThread().getName()+"] Prepared meta data. Will begin loading ..");
        
			}
		} catch (SQLException e) {
			throw e;
		}
	}
	
  private SimpleDateFormat[] dateFormats;
	private Map<Integer, ColumnMeta> dataTypes = new TreeMap<>();
	private Map<Short, String> primaryKeys = new TreeMap<>();
	
	private void loadDateFormats()
	{
	  if(dateFormats == null)
	  {
	    if(System.getProperty("date.formats") != null)
	    {
	      try 
	      {
          String[] formats = System.getProperty("date.formats").split(",");
          dateFormats = new SimpleDateFormat[formats.length];
          for(int i=0; i<formats.length; i++)
          {
            dateFormats[i] = new SimpleDateFormat(formats[i]);
          }
          return;
        } catch (Exception e) {
          log.warning("["+Thread.currentThread().getName()+"] Ignoring invalid date.formats provided. Using default ISO 8601.");
        }
	    }
	    dateFormats = new SimpleDateFormat[ConfigLoader.ISO_8601_DATE_FORMATS.length];
	    for(int i=0; i<ConfigLoader.ISO_8601_DATE_FORMATS.length; i++)
	    {
	      dateFormats[i] = new SimpleDateFormat(ConfigLoader.ISO_8601_DATE_FORMATS[i]);
	    }
	  }
	}
	private boolean autoIncrement = false;
	private void reflectDataTypes(ResultSet result) throws SQLException {
	  String colName;
	  int type, ordinal, size;
	  
	  while(result.next())
    {
      colName = result.getString(4);
      type = result.getInt(5);
      ordinal = result.getInt(17);
      size = result.getInt(7);
      if (!autoIncrement && "YES".equalsIgnoreCase(result.getString(23))) {
        autoIncrement = true;
        continue;
      }
      if(autoIncrement)
        ordinal--;
      
      boolean notNull = "no".equalsIgnoreCase(result.getString(18));
      
      switch(type)
      {
        case Types.DATE:
          loadDateFormats();
          dataTypes.put(ordinal, new ColumnMeta(Date.class, colName, Types.DATE, notNull));
          break;
        case Types.TIMESTAMP:
          loadDateFormats();
          dataTypes.put(ordinal, new ColumnMeta(Timestamp.class, colName, Types.TIMESTAMP, notNull));
          break;
        case Types.TIME:
          loadDateFormats();
          dataTypes.put(ordinal, new ColumnMeta(Time.class, colName, Types.TIME, notNull));
          break;
        case Types.TINYINT:
          dataTypes.put(ordinal, new ColumnMeta(Integer.class, colName, Types.TINYINT, notNull));
          break;
        case Types.SMALLINT:
          dataTypes.put(ordinal, new ColumnMeta(Integer.class, colName, Types.SMALLINT, notNull));
          break;
        case Types.INTEGER:
          dataTypes.put(ordinal, new ColumnMeta(Integer.class, colName, Types.INTEGER, notNull));
          break;
        case Types.BIGINT:
          dataTypes.put(ordinal, new ColumnMeta(Long.class, colName, Types.BIGINT, notNull));
          break;
        case Types.REAL:
          dataTypes.put(ordinal, new ColumnMeta(Double.class, colName, Types.REAL, notNull));
          break;
        case Types.DOUBLE:
          dataTypes.put(ordinal, new ColumnMeta(Double.class, colName, Types.DOUBLE, notNull));
          break;
        case Types.FLOAT:
          dataTypes.put(ordinal, new ColumnMeta(Double.class, colName, Types.FLOAT, notNull));
          break;
        case Types.DECIMAL:
          dataTypes.put(ordinal, new ColumnMeta(Double.class, colName, Types.DECIMAL, notNull));
          break;
        case Types.NUMERIC:
          dataTypes.put(ordinal, new ColumnMeta(Double.class, colName, Types.NUMERIC, notNull));
          break;
        case Types.CLOB:
          dataTypes.put(ordinal, new ColumnMeta(String.class, colName, Types.CLOB, notNull));
          break;
        case Types.CHAR:
          dataTypes.put(ordinal, new ColumnMeta(String.class, colName, Types.CHAR, notNull));
          break;
        case Types.VARCHAR:
          dataTypes.put(ordinal, new ColumnMeta(String.class, colName, Types.VARCHAR, notNull));
          break;
        case Types.LONGVARCHAR:
          dataTypes.put(ordinal, new ColumnMeta(String.class, colName, Types.LONGVARCHAR, notNull));
          break;
         default:
           throw new UnsupportedOperationException("Data type not supported for column: "+colName+" java.sql.Types: "+type);
      }
      
      dataTypes.get(ordinal).size = size;
      
    }
    
  }
	private AtomicInteger batchCount = new AtomicInteger(0);
	/**
	 * Adds the next record to batch
	 * @param values
	 * @param jobIndex 
	 * @throws SQLException
	 */
  public void addBatch(String[] values, int jobIndex) throws SQLException{
    if(values.length != dataTypes.size())
      throw new SQLException(new IllegalArgumentException("[Rec#"+jobIndex+"] Input params size ("+values.length+") do not match DB column size ("+dataTypes.size()+")"));
		
    try {
      addBatchWithDataType(values, jobIndex);
    } catch (SQLException e) {
      log.severe("At record index: "+jobIndex);
      throw e;
    }
		
		if(batchCount.incrementAndGet() >= batchSize)
		{
		  executeBatch();
		  batchCount.getAndSet(0);
		}
	}
  private java.util.Date toDate(String date)
  {
    try {
      long time = Long.parseLong(date);
      return new java.util.Date(time);
    } catch (NumberFormatException e) {
      for(SimpleDateFormat df : dateFormats)
      {
        try {
          return df.parse(date);
        } catch (ParseException e1) {
          
        }
      }
    }
    return null;
    
  }
  @SuppressWarnings("unused")
  private void addBatchAsString(String[] values){
    try
    {
      if(insertPstmt != null)
      {
        
        for(int i=0; i<values.length; i++){
          insertPstmt.setString(i+1, values[i]);    // assumes all strings
        }
        insertPstmt.addBatch();
      }
    } catch (SQLException e) {
      System.err.println(Arrays.toString(values));
      e.printStackTrace();
    }
  }
  /**
   * @deprecated
   * @param values
   * @param jobIndex
   * @return
   * @throws SQLException
   */
  private Map<String, String> getInsertParams(String[] values, int jobIndex) throws SQLException
  {
    Map<String, String> params = new LinkedHashMap<>();
    java.util.Date javaDate;
    for(int i=0; i<values.length; i++)
    {
      if(dataTypes.containsKey(i+1))
      {
        Class<?> type = dataTypes.get(i+1).type;
        
        if(System.getProperty("skip.blank.field") != null && (values[i] == null || values[i].isEmpty()))
        {
          log.warning("[Rec#"+jobIndex+"] Skipping record with blank at param index "+(i+1)+". Value ["+values[i]+"]");
          return null;
        }
        
        if(type == Integer.class)
        {
          try {
            params.put(dataTypes.get(i+1).name, Integer.valueOf(values[i]).toString());
          } catch (NumberFormatException e) {
            log.info("[Rec#"+jobIndex+"] ignoring invalid number "+values[i]);
            if(System.getProperty("skip.invalid.field") == null)
              params.put(dataTypes.get(i+1).name,"NULL");
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              return null;
            }
              
          }
        }
        else if(type == Long.class)
        {
          try {
            params.put(dataTypes.get(i+1).name,Long.valueOf(values[i]).toString());
          } catch (NumberFormatException e) {
            log.info("[Rec#"+jobIndex+"] ignoring invalid number "+values[i]);
            if(System.getProperty("skip.invalid.field") == null)
              params.put(dataTypes.get(i+1).name,"NULL");
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              return null;
            }
          }
        }
        else if(type == Double.class)
        {
          try {
            params.put(dataTypes.get(i+1).name,Double.valueOf(values[i]).toString());
          } catch (NumberFormatException e) {
            log.info("[Rec#"+jobIndex+"] ignoring invalid number "+values[i]);
            if(System.getProperty("skip.invalid.field") == null)
              params.put(dataTypes.get(i+1).name,"NULL");
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              return null;
            }
          }
        }
        else if(type == Time.class)
        {
          javaDate = toDate(values[i]);
          if(javaDate == null){
            log.info("[Rec#"+jobIndex+"] ignoring unparseable date "+values[i]);
            if(System.getProperty("skip.invalid.field") == null)
              params.put(dataTypes.get(i+1).name,"NULL");
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              return null;
            }
          }
          else
          {
            //insertPstmt.setObject(i+1, new Time(javaDate.getTime()));
            throw new UnsupportedOperationException("Cannot handle date/time types in a vendor independent manner!");
          }
          
        }
        else if(type == Timestamp.class)
        {
          javaDate = toDate(values[i]);
          if(javaDate == null){
            log.info("[Rec#"+jobIndex+"] ignoring unparseable date "+values[i]);
            if(System.getProperty("skip.invalid.field") == null)
              params.put(dataTypes.get(i+1).name,"NULL");
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              return null;
            }
          }
          else
          {
            //insertPstmt.setObject(i+1, new Timestamp(javaDate.getTime()));
            throw new UnsupportedOperationException("Cannot handle date/time types in a vendor independent manner!");
          }
        }
        else if(type == Date.class)
        {
          javaDate = toDate(values[i]);
          if(javaDate == null){
            log.info("[Rec#"+jobIndex+"] ignoring unparseable date "+values[i]);
            if(System.getProperty("skip.invalid.field") == null)
              params.put(dataTypes.get(i+1).name,"NULL");
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              return null;
            }
          }
          else
          {
            //insertPstmt.setObject(i+1, new Date(javaDate.getTime()));
            throw new UnsupportedOperationException("Cannot handle date/time types in a vendor independent manner!");
          }
        }
        else
        {
          
          //string
          params.put(dataTypes.get(i+1).name,"'"+values[i]+"'");
          
        }
      }
      else
      {
        throw new SQLException("Found record with unexpected data type at param index "+(i+1)+". Value ["+values[i]+"]");
      }
          
    }
    return params;
    
  }
  /**
   * best effort to cast into the target data type
   * @param values
   * @param jobIndex
   * @throws SQLException
   */
  private void addInsertToBatch(String[] values, int jobIndex) throws SQLException
  {
    java.util.Date javaDate;
    for(int i=0; i<values.length; i++)
    {
      
      if(dataTypes.containsKey(i+1))
      {
        Class<?> type = dataTypes.get(i+1).type;
        
        if(System.getProperty("skip.blank.field") != null && (values[i] == null || values[i].isEmpty()))
        {
          log.warning("[Rec#"+jobIndex+"] Skipping record with blank at param index "+(i+1)+". Value ["+values[i]+"]");
          insertPstmt.clearParameters();
          return;
        }
        if(type == Integer.class || type == Long.class)
        {
          try 
          {
            BigInteger bigInt = new BigInteger(values[i].contains(".") ? 
                values[i].substring(0, values[i].indexOf('.')) : values[i]);
            insertPstmt.setObject(i+1, type == Integer.class ? bigInt.intValue() : bigInt.longValue());
          } catch (Exception e) {
            log.fine("[Rec#"+jobIndex+"] ignoring invalid number ("+values[i]+")");
            if(System.getProperty("skip.invalid.field") == null)
              insertPstmt.setNull(i+1, dataTypes.get(i+1).sqlType);
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              insertPstmt.clearParameters();
              return;
            }
              
          }
        }
        else if(type == Double.class)
        {
          try {
            insertPstmt.setObject(i+1, new BigDecimal(values[i]).doubleValue());
          } catch (Exception e) {
            log.fine("[Rec#"+jobIndex+"] ignoring invalid number ("+values[i]+")");
            if(System.getProperty("skip.invalid.field") == null)
              insertPstmt.setNull(i+1, dataTypes.get(i+1).sqlType);
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              insertPstmt.clearParameters();
              return;
            }
          }
          }
        else if(type == Time.class)
        {
          javaDate = toDate(values[i]);
          if(javaDate == null){
            log.fine("[Rec#"+jobIndex+"] ignoring unparseable date ("+values[i]+")");
            if(System.getProperty("skip.invalid.field") == null)
              insertPstmt.setNull(i+1, dataTypes.get(i+1).sqlType);
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              insertPstmt.clearParameters();
              return;
            }
          }
          else
          {
            insertPstmt.setObject(i+1, new Time(javaDate.getTime()));
          }
          
        }
        else if(type == Timestamp.class)
        {
          javaDate = toDate(values[i]);
          if(javaDate == null){
            log.fine("[Rec#"+jobIndex+"] ignoring unparseable date ("+values[i]+")");
            if(System.getProperty("skip.invalid.field") == null)
              insertPstmt.setNull(i+1, dataTypes.get(i+1).sqlType);
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              insertPstmt.clearParameters();
              return;
            }
          }
          else
          {
            insertPstmt.setObject(i+1, new Timestamp(javaDate.getTime()));
          }
        }
        else if(type == Date.class)
        {
          javaDate = toDate(values[i]);
          if(javaDate == null){
            log.fine("[Rec#"+jobIndex+"] ignoring unparseable date ("+values[i]+")");
            if(System.getProperty("skip.invalid.field") == null)
              insertPstmt.setNull(i+1, dataTypes.get(i+1).sqlType);
            else{
              log.warning("[Rec#"+jobIndex+"] Skipping record with unexpected value at param index "+(i+1)+". Value ["+values[i]+"]");
              insertPstmt.clearParameters();
              return;
            }
          }
          else
          {
            insertPstmt.setObject(i+1, new Date(javaDate.getTime()));
          }
        }
        else
        {
          //string
          insertPstmt.setObject(i+1, (values[i].length() > dataTypes.get(i+1).size) ? 
              values[i].substring(0, dataTypes.get(i+1).size) : values[i]);
          
        }
      }
      else
      {
        throw new SQLException("Found record with unexpected data type at param index "+(i+1)+". Value ["+values[i]+"]");
      }
          
    }
    insertPstmt.addBatch();
  
  }
  private void addBatchWithDataType(String[] values, int jobIndex) throws SQLException{
    try
    {
      if((System.getProperty("upsert") != null))
      {
        addDelInsertToBatch(values, jobIndex);
      }
      else
      {
        addInsertToBatch(values, jobIndex);
      }
    } catch (SQLException e) {
      throw e;
    }
  }
  /**
   * @deprecated not optimized.
   * @param values
   * @param jobIndex
   * @throws SQLException
   */
  private void addDelInsertToBatch(String[] values, int jobIndex) throws SQLException
  {
    try
    {
      Map<String, String> params = getInsertParams(values, jobIndex);
      List<String> delParams = new ArrayList<>();
      if(params != null)
      {
        if(!primaryKeys.isEmpty() && !autoIncrement)
        {
          //prepare delete
          for(String col : primaryKeys.values())
          {
            if(params.containsKey(col))
            {
              delParams.add(params.get(col));
            }
          }
          String delQry = deleteSQL;
          for(String p : delParams)
          {
            delQry = delQry.replaceFirst("\\?", p);
          }
          log.fine(delQry);
          batchStmt.addBatch(delQry);
        }
        String insQry = insertSQL;
        for(String p : params.values())
        {
          insQry = insQry.replaceFirst("\\?", p);
        }
        log.fine(insQry);
        batchStmt.addBatch(insQry);
      }
    } catch (SQLException e) {
      throw e;
    }
  }
  static void reflectSQL(Statement stmt)
  {
    try {
      for(Field f : stmt.getClass().getDeclaredFields())
      {
        f.setAccessible(true);
        Object o = f.get(stmt);
        if(o instanceof String)
        {
          if (((String) o).toUpperCase().contains("INSERT")
              || ((String) o).toUpperCase().contains("DELETE")
              || ((String) o).toUpperCase().contains("UPDATE")) {
            
            log.info(o.toString());

          }
        }
      }
    } catch (Exception e) {
      
    } 
  }
  private int[] executeBatch0(Statement aStatement) throws SQLException
  {
    int[] count = null;
    boolean commit = false;
    try
    {
      try 
      {
        count = aStatement.executeBatch();
        commit = true;          
      } catch (BatchUpdateException e) {
        count = e.getUpdateCounts();
        log.log(Level.SEVERE, "["+Thread.currentThread().getName()+"] Batch execution exception => { "+e.getMessage());    
        log.log(Level.FINE, "-- Stacktrace --", e);
        if("true".equalsIgnoreCase(ConfigLoader.getConfig().getProperty(ConfigLoader.COMMIT_ON_BATCH_FAIL, "true")))
        {
          commit = true;
        }
      }
      
    } catch (SQLException e) {
      throw e;
    }
    finally
    {
      if(commit)
      {
        conn.commit();
        List<Integer> errs = new ArrayList<>();
        aStatement.clearBatch();
        if (count != null) {
          int j = 0;
          for (int i : count) {
            j++;
            if (i == Statement.EXECUTE_FAILED) {
              errs.add(j);
            } 
            else if(i == Statement.SUCCESS_NO_INFO)
            {
              log.warning("["+Thread.currentThread().getName()+"] Statement.SUCCESS_NO_INFO " + j);
            }
            else 
            {
              
              if(System.getProperty("upsert") != null){
                if(j % 2 == 0)
                  counter.incrementAndGet();
              }
              else{
                counter.incrementAndGet();
              }
            }

          } 
          if(!errs.isEmpty())
          {
            log.severe("\tFailed record count: "+errs.size());
            log.fine("\tFailed record offset: "+errs);
            log.severe("}");
          }
        }
      }
      else
      {
        conn.rollback();
        aStatement.clearBatch();
      }
      
    }
    return count;
  }
	
  /**
	 * 
	 * @return
	 * @throws SQLException
	 */
	public int[] executeBatch() throws SQLException{
	  if(System.getProperty("upsert") != null)
	    return executeBatch0(batchStmt);
	  else
	    return executeBatch0(insertPstmt);
	}

	public int executeUpdate(){
		int count = 0;
		try{
			if(insertPstmt != null){
				count = insertPstmt.executeUpdate();
				conn.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}

	public ResultSet executeQuery(){
		try{
			if(insertPstmt != null){
				rs = insertPstmt.executeQuery();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	public ResultSet executeQuery(String sql){
		try{
			if(rs != null){
				rs.close();
				rs = null;
			}
			if(stmt != null){
				stmt.close();
				stmt = null;
			}
			if(sql != null && conn != null){
				stmt = conn.createStatement();
				rs = stmt.executeQuery(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	public int executeUpdate(String sql){
		int count = 0;
		try{
			if(rs != null){
				rs.close();
				rs = null;
			}
			if(stmt != null){
				stmt.close();
				stmt = null;
			}
			if(sql != null && conn != null){
				stmt = conn.createStatement();
				count = stmt.executeUpdate(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}

	public void close(){
		try {
			if(rs != null){
				rs.close();
				rs = null;
			}
			if(insertPstmt != null){
				insertPstmt.close();
				insertPstmt = null;
			}
			if(stmt != null){
				stmt.close();
				stmt = null;
			}
			if(batchStmt != null){
			  batchStmt.close();
			  batchStmt = null;
      }
			if(conn != null){
				conn.close();
				conn = null;
			}
			log.finer("Disconnected from database..");
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

}
