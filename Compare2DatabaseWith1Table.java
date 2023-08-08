import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Compare2DatabaseWith1Table {
	
    public static void main(String[] args) {
        // Connection details for source database
        String sourceJdbcUrl = "jdbc:oracle:thin:@HOST:1521:SERVICE_NAME";
        String sourceUsername = "USERNAME";
        String sourcePassword = "PASSWORD";

        // Connection details for target database
        String targetJdbcUrl = "jdbc:oracle:thin:@HOST:1521:SERVICE_NAME";
        String targetUsername = "USERNAME";
        String targetPassword = "PASSWORD";

        // Table name to compare
        String tableName = "user_cart";

        try {
            // Load the Oracle JDBC driver
            Class.forName("oracle.jdbc.OracleDriver");

            // Establish connections for source and target databases
            Connection sourceConnection = DriverManager.getConnection(sourceJdbcUrl, sourceUsername, sourcePassword);
            Connection targetConnection = DriverManager.getConnection(targetJdbcUrl, targetUsername, targetPassword);

            // Create statements for source and target databases
            Statement sourceStatement = sourceConnection.createStatement();
            Statement targetStatement = targetConnection.createStatement();
            
            // Calculate the date that is 1250 days ago from today
            LocalDate thirtyDaysAgo = LocalDate.now().minusDays(1250);
            System.out.println("LocalDate: "+thirtyDaysAgo);
            
            // Format the date in the format expected by Oracle (assuming the column is of type DATE)
            String formattedDate = thirtyDaysAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            
            // Execute queries to retrieve data from the table in both databases
            String sourceQuery = "SELECT cart_id, node_id, batch_no FROM " + tableName + " WHERE createts >= TO_DATE('" + formattedDate + "', 'YYYY-MM-DD')";
            String targetQuery = "SELECT cart_id, node_id, batch_no FROM " + tableName + " WHERE createts >= TO_DATE('" + formattedDate + "', 'YYYY-MM-DD')";

            ResultSet sourceResultSet = sourceStatement.executeQuery(sourceQuery);
            ResultSet targetResultSet = targetStatement.executeQuery(targetQuery);

            // Compare the result sets
            boolean areRecordsEqual = compareResultSets(sourceResultSet, targetResultSet);
            
            // Print the comparison result
            if (areRecordsEqual) {
                System.out.println("Records in table " + tableName + " are the same in both databases.");
            } else {
                System.out.println("Records in table " + tableName + " are different between the databases.");
            }

            // Close resources
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

    // Helper method to compare two result sets
    private static boolean compareResultSets(ResultSet rs1, ResultSet rs2) throws Exception {
        // Compare result sets row by row
        while (rs1.next() && rs2.next()) {
            int numColumns = rs1.getMetaData().getColumnCount();
            for (int i = 1; i <= numColumns; i++) {
                if (!rs1.getObject(i).equals(rs2.getObject(i))) {
                    return false; // Records are different
                }
            }
        }
        
        // Check if both result sets have the same number of rows
        return !(rs1.next() || rs2.next());
    }
}

