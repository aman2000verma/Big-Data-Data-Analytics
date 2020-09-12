package ddl;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Enables a particular table
 */
public class EnableTable {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Enable Table\nEnter Table Name: ");
		String name = scan.nextLine();
		scan.close();

		// Enable table if its currently disabled
		try {
			if (admin.isTableEnabled(name))
				System.out.println("Table already enabled");
			else {
				admin.enableTable(name);
				System.out.println("Table enabled successfully");
			}

		} catch (TableNotFoundException e) {
			// If table not found, print exception
			System.out.println("Table not found\n" + e.toString());
		}

		admin.close();
	}

}
