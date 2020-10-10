package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Write a HiveQL using Java API to create a table for Olympic data set.
 */
public class Q1 {

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

		// Creating table for Olympic data set
		String createOly = "create table olympic ("
				+ "athlete_name string, age int, country string, year int, closing_date string, sport string, gold int, silver int, bronze int, total int"
				+ ")" + " row format delimited" + " fields terminated by ','";

		try {
			statement.execute(createOly);
			System.out.println("Created table olympic.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}
