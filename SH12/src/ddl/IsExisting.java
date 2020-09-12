package ddl;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Checks whether table exist
 */
public class IsExisting {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Status\nEnter Table Name: ");
		String name = scan.nextLine();
		scan.close();

		if (admin.tableExists(name))
			System.out.println("Table Exist: TRUE");
		else
			System.out.println("Table Exist: FALSE");

		admin.close();
	}

}
