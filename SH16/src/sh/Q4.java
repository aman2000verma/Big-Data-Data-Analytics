package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Write a HiveQL using Java API to create a bucket table by country and load
 * data into partition table from Olympic Table.
 */
public class Q4 {

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

		// Creating bucket table by country
		String createBucket = "create table countryBucket ("
				+ "athlete_name string, age int, country string, year int, closing_date string, sport string, gold int, silver int, bronze int, total int"
				+ ") clustered by (country) into 10 buckets"
				+ " row format delimited" + " fields terminated by ','";

		try {
			statement.execute(createBucket);
			System.out.println("Created bucket table.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		// insert data into bucket table
		String loadBucket = "insert overwrite table countryBucket select * from olympic";

		try {
			statement.execute(loadBucket);
			System.out.println("Data loaded.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}
