package ddl;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Disables a particular table
 */
public class DisableTable {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Disable Table\nEnter Table Name: ");
		String name = scan.nextLine();
		scan.close();

		// Disable table if its currently enabled
		try {
			if (admin.isTableDisabled(name))
				System.out.println("Table already disabled");
			else {
				admin.disableTable(name);
				System.out.println("Table disabled successfully");
			}

		} catch (TableNotFoundException e) {
			// If table not found, print exception
			System.out.println("Table not found\n" + e.toString());
		}

		admin.close();
	}

}
