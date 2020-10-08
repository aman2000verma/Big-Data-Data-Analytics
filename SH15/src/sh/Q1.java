package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Write a HiveQL using Java API to create a table for Employee and Salary data
 * set.
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

		// Creating table for Employee data set
		String createEmp = "create table emp ("
				+ "emp_id	int, birthday string, first_name string, last_name string, gender string, work_day string"
				+ ")" + " row format delimited" + " fields terminated by ','";

		try {
			statement.execute(createEmp);
			System.out.println("Created table emp.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		// Creating table for Salary data set
		String createSalary = "create table emp_salary ("
				+ "emp_id int, salary string, start_date string, end_date string"
				+ ")" + " row format delimited" + " fields terminated by ','";

		try {
			statement.execute(createSalary);
			System.out.println("Created table emp_salary.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}
