import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConnectOracleAndFetchRecordCount {
    public static void main(String[] args) {
        // Connection details
        String jdbcUrl = "jdbc:oracle:thin:@HOST:1521:SERVICE_NAME";
        String username = "USERNAME";
        String password = "PASSWORD";

        // Table name
        String tableName = "user_cart";

        try {
            // Load the Oracle JDBC driver
            Class.forName("oracle.jdbc.OracleDriver");

            // Establish the connection
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);

            // Create a statement
            Statement statement = connection.createStatement();

            // Execute a query to get record count
            String query = "SELECT COUNT(*) FROM " + tableName;
            ResultSet resultSet = statement.executeQuery(query);

            // Fetch and print the record count
            if (resultSet.next()) {
                int recordCount = resultSet.getInt(1);
                System.out.println("Record count in table " + tableName + ": " + recordCount);
            }

            // Close resources
            resultSet.close();
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
