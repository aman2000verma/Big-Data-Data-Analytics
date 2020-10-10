package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Write a HiveQL using Java API to find out total number of medals won by each
 * country.
 */
public class Q5 {

	// Driver Class
	private static String driverClass = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException exception) {
			System.out.println(exception.toString());
			System.exit(1);
		}

		// Creating a connection with Hive using JDBC
		Connection connection = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000/default", "hive", "");

		// Creating statement instance with the created connection
		Statement statement = connection.createStatement();

		// Query
		String query = "select country, sum(total) from olympic"
				+ " group by country";

		ResultSet res = null;
		try {
			res = statement.executeQuery(query);
			ResultSetMetaData meta = res.getMetaData();

			int numCols = meta.getColumnCount();

			System.out.println("Total medals won by each country");
			System.out.println("Country, Num of Medals");
			while (res.next()) {
				for (int i = 1; i <= numCols; i++) {
					// Print the columns separated by commas and create new line
					// after the last column is read
					if (i != numCols)
						System.out.print(res.getString(i) + ", ");
					else
						System.out.print(res.getString(i) + "\n");
				}
			}
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}
