package sh;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Write a HiveQL using Java API for loading data into Employee and Salary
 * Tables from the given the data set files.
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
		String loadEmp = "load data local inpath '/home/cloudera/Desktop/employee.csv' overwrite into table emp";
		try {
			statement.execute(loadEmp);
			System.out.println("Data loaded into table emp.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		// Loading data from CSV file into table
		String loadSalary = "load data local inpath '/home/cloudera/Desktop/salary.csv' overwrite into table emp_salary";
		try {
			statement.execute(loadSalary);
			System.out.println("Data loaded into table emp_salary.");
		} catch (SQLException ex) {
			System.out.println(ex.toString());
		}

		statement.close();
		connection.close();
	}
}
