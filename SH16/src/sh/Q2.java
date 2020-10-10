package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Write a HiveQL using Java API for loading data into Olympic Table from the
 * given the data set file.
 */
public class Q2 {

	/**
	 * The data sets were stored at '/home/cloudera/Desktop/' during the
	 * execution of the class.
	 */

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

		// Loading data from CSV file into table
		String load = "load data local inpath '/home/cloudera/Desktop/olympic_data.csv' overwrite into table olympic";
		try {
			statement.execute(load);
			System.out.println("Data loaded into table olympic.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}
