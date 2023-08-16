import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.util.Properties;

public class Compare2DatabaseWith1TableWithPropertyLogs {
	
    public static void main(String[] args) {
    	
    	//Load Property field and read the config data
    	Properties properties = loadProperties("DatabaseConfig.properties");
    	    	
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
            System.out.println("Days to Compare: "+daysToCompare);
            System.out.println("LocalDate: "+thirtyDaysAgo);
            
            // Format the date in the format expected by Oracle (assuming the column is of type DATE)
            String formattedDate = thirtyDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            // Retrieve the primary key column name for the table
            System.out.println("TableName: "+tableName);
            String primaryKeyColumnName = getPrimaryKeyColumnName(sourceConnection, tableName);
            
            System.out.println("PrimaryKey Field: "+primaryKeyColumnName);
            
            // Execute queries to retrieve data from the table in both databases
            String sourceQuery = "SELECT * FROM " + tableName + " WHERE createts >= TO_DATE('" + formattedDate + "', 'YYYY-MM-DD')";
            String targetQuery = "SELECT * FROM " + tableName + " WHERE createts >= TO_DATE('" + formattedDate + "', 'YYYY-MM-DD')";
            System.out.println("sourceQuery: "+sourceQuery);
            System.out.println("targetQuery: "+targetQuery);
            
            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceQuery);
            ResultSet targetResultSet = targetStatement.executeQuery(targetQuery);

            // Create a PrintWriter to write to a .txt or .log file
            PrintWriter writer = new PrintWriter(new FileWriter("differences.log"));

            // Get the metadata to retrieve column names
            ResultSetMetaData metaData = sourceResultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Compare result sets row by row 
            while (sourceResultSet.next() && targetResultSet.next()) {
                boolean areRowsEqual = true;
                StringBuilder diffMessage = new StringBuilder("Differences in row:\n");

                for (int i = 1; i <= columnCount; i++) {
                    Object sourceValue = sourceResultSet.getObject(i);
                    Object targetValue = targetResultSet.getObject(i);

                    if (!sourceValue.equals(targetValue)) {
                        areRowsEqual = false;
                        diffMessage.append(metaData.getColumnName(i))
                                .append(": Source=")
                                .append(sourceValue)
                                .append(", Target=")
                                .append(targetValue)
                                .append("\n");
                    }
                }

                if (!areRowsEqual) {
                	String primaryKeyValue = sourceResultSet.getString(primaryKeyColumnName);
                	writer.println((Integer.toString(differenceCounter++)) + ". "+primaryKeyColumnName+": " + primaryKeyValue);
                }
            }

         // Close resources
            writer.close();
            sourceResultSet.close();
            targetResultSet.close();
            sourceStatement.close();
            targetStatement.close();
            sourceConnection.close();
            targetConnection.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    
    
    /* Identify the PrimaryKey field name for the passed Table name */
    public static String getPrimaryKeyColumnName(Connection connection, String tableName) throws Exception {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);

        if (primaryKeys.next()) {
            return primaryKeys.getString("COLUMN_NAME");
        }

        throw new Exception("Primary key not found for table: " + tableName);
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

}

