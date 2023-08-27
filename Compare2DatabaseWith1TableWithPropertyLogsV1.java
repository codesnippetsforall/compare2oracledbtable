package com.samples.database;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.util.Properties;

public class Compare2DatabaseWith1TableWithPropertyLogs3 {
	
    public static void main(String[] args) {
    	
    	//Load Property field and read the config data
    	Properties properties = loadProperties("DatabaseConfig2.properties");
    	    	
    	// Connection details for source database
        String sourceJdbcUrl = properties.getProperty("sourceJdbcUrl");
        String sourceUsername = properties.getProperty("sourceUsername");
        String sourcePassword = properties.getProperty("sourcePassword");
        
        // Connection details for target database
        String targetJdbcUrl = properties.getProperty("targetJdbcUrl");
        String targetUsername = properties.getProperty("targetUsername");
        String targetPassword = properties.getProperty("targetPassword");
        
        // Table name to Compare
    	String tableName = properties.getProperty("tableName").toUpperCase();
    	String pKeyFieldName = properties.getProperty("pKeyFieldName").toUpperCase();
    	String dateFiledtoCheck = properties.getProperty("dateFiledtoCheck");
    	
    	// Trace
    	String enableDetailedTrace = properties.getProperty("enableDetailedTrace");    	
    	
    	// Days to Compare
    	int daysToCompare = Integer.parseInt(properties.getProperty("daysToCompare"));
        
        try {
        	           
            //Counter the print the sequence of how many rows having difference
            int differenceCounter = 1;
            
            // Load the Oracle JDBC driver
            Class.forName("oracle.jdbc.OracleDriver");

            // Establish connections for source and target databases
            Connection sourceConnection = DriverManager.getConnection(sourceJdbcUrl, sourceUsername, sourcePassword);
            Connection targetConnection = DriverManager.getConnection(targetJdbcUrl, targetUsername, targetPassword);

            // Create statements for source and target databases
            Statement sourceStatement = sourceConnection.createStatement();
            Statement targetStatement = targetConnection.createStatement();
            
            // Calculate the date that is 30 days ago from today
            LocalDate thirtyDaysAgo = LocalDate.now().minusDays(daysToCompare);
            
            if (enableDetailedTrace.equals("Y")) System.out.println("Days to Compare: "+daysToCompare);
            if (enableDetailedTrace.equals("Y")) System.out.println("LocalDate: "+thirtyDaysAgo);
            
            // Format the date in the format expected by Oracle (assuming the column is of type DATE)
            String formattedDate = thirtyDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));            
            
            // Retrieve the primary key column name for the table
            if (enableDetailedTrace.equals("Y")) System.out.println("TableName: "+tableName);
            
            String primaryKeyColumnName = getPrimaryKeyColumnName(sourceConnection, tableName);                       
            if (enableDetailedTrace.equals("Y")) System.out.println("PrimaryKey Field: "+primaryKeyColumnName);
            
            /* Validate PrimaryKey column */
            validatePrimaryKeyColumn(pKeyFieldName, primaryKeyColumnName);
            
            // Execute queries to retrieve data from the table in both databases
            String sourceQuery = "SELECT * FROM " + tableName + " WHERE "+ dateFiledtoCheck +" >= TO_DATE('" + formattedDate + "', 'YYYY-MM-DD')";
            String targetQuery = "SELECT * FROM " + tableName + " WHERE "+ dateFiledtoCheck +" >= TO_DATE('" + formattedDate + "', 'YYYY-MM-DD')";
            if (enableDetailedTrace.equals("Y")) System.out.println("sourceQuery: "+sourceQuery);
            if (enableDetailedTrace.equals("Y")) System.out.println("targetQuery: "+targetQuery);
            
            int sourceRecordCount = getRecordCount(sourceStatement, sourceQuery);
            int targetRecordCount = getRecordCount(targetStatement, targetQuery);

            if (enableDetailedTrace.equals("Y")) System.out.println("Source Record Count: " + sourceRecordCount);
            if (enableDetailedTrace.equals("Y")) System.out.println("Target Record Count: " + targetRecordCount);
            
            /* Validate Source & Target Records count */
            validateSourceTargetRecords(sourceRecordCount, targetRecordCount);
            
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceQuery);

            // Create a PrintWriter to write to a .txt or .log file
            PrintWriter writer = new PrintWriter(new FileWriter("differences.log"));

            ResultSetMetaData metaData = sourceResultSet.getMetaData();
            int columnCount = metaData.getColumnCount();  // Define columnCount here

            // Iterate through all source records
            while (sourceResultSet.next()) {
                String sourcePrimaryKeyValue = sourceResultSet.getString(primaryKeyColumnName);

                ResultSet targetResultSet = targetStatement.executeQuery(targetQuery);

                // Compare the primary key value with all target records
                while (targetResultSet.next()) {
                    String targetPrimaryKeyValue = targetResultSet.getString(primaryKeyColumnName);

                    if (sourcePrimaryKeyValue.equals(targetPrimaryKeyValue)) {
                        boolean areRowsEqual = true;
                        StringBuilder diffMessage = new StringBuilder("Differences in row:\n");

                        for (int i = 1; i <= columnCount; i++) {
                            Object sourceValue = sourceResultSet.getObject(i);
                            Object targetValue = targetResultSet.getObject(i);

                            String ColumnName = metaData.getColumnName(i);

                            if (enableDetailedTrace.equals("Y")) System.out.println("Source Column Name :"+ ColumnName+" : "+sourceValue);
                            if (enableDetailedTrace.equals("Y")) System.out.println("Target Column Name :"+ ColumnName+" : "+targetValue);

                            if (sourceValue != null && !sourceValue.equals(targetValue)) {
                                areRowsEqual = false;
                                diffMessage.append(ColumnName)
                                        .append(": Source=")
                                        .append(sourceValue)
                                        .append(", Target=")
                                        .append(targetValue)
                                        .append("\n");
                            }
                        }

                        /* Write difference log where source & target record NOT matching */
                        if (!areRowsEqual) {
                            writer.println((Integer.toString(differenceCounter++)) + ". "+primaryKeyColumnName+": " + sourcePrimaryKeyValue);

                            if (enableDetailedTrace.equals("Y")) System.out.println("~~~~~~~~ Source Record != Target Record ~~~~~~~~ Primary Key: " + sourcePrimaryKeyValue);
                        }
                    }
                }

                targetResultSet.close();
            }

            // Close resources
            writer.close();
            sourceResultSet.close();
            sourceStatement.close();
            targetStatement.close();
            sourceConnection.close();
            targetConnection.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    
    
    /* Validate Source & Target Record Count */
    private static void validateSourceTargetRecords(int sourceRecordCount, int targetRecordCount) throws Exception {
		
    	if (sourceRecordCount > targetRecordCount) {
    		throw new Exception("Custom Exception: Source Record(s) count greater than Target Record(s), \n "
    				+ "Cannot proceed with Recordwise comparison ! ");
    	} else if (sourceRecordCount < targetRecordCount) {
    		throw new Exception("Custom Exception: Source Record(s) count lesser than Target Record(s), \n "
    				+ "Cannot proceed with Recordwise comparison ! ");
    	}
		
	}

	/* Validate Primary Key Column */
    private static void validatePrimaryKeyColumn(String pKeyFieldName, String primaryKeyColumnName) throws Exception {
		
    	if (pKeyFieldName == null || pKeyFieldName.isEmpty()) {
    		throw new Exception("Custom Exception: Primary key field should be defined in the Config File ! ");
    	} else if (!pKeyFieldName.equals(primaryKeyColumnName)) {
    		throw new Exception("Custom Exception: Primary key field defined in the Config file and Primary Key matched in table not matching ! \n "
    				+ "Please verify the config file and update to proceed ! ");
    	}
		
	}

	/* Identify the PrimaryKey field name for the passed Table name */
    public static String getPrimaryKeyColumnName(Connection connection, String tableName) throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);

        if (primaryKeys.next()) {
            return primaryKeys.getString("COLUMN_NAME");
        }

        throw new Exception("Custom Exception: Primary key not found for table: " + tableName);
    }

    /* Load the property file and return the details */
    public static Properties loadProperties(String fileName) {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream(fileName)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
    
    /* Method to get the record count from a result set */
    private static int getRecordCount(Statement statement, String query) {
        int count = 0;
        try {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                count++;
            }
            resultSet.close(); // Close the result set
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

}
