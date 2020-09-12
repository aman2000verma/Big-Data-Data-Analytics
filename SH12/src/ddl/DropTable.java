package ddl;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Drops a particular table
 */
public class DropTable {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Drop Table\nEnter Table Name: ");
		String name = scan.nextLine();
		scan.close();

		// Disable and drop the table
		try {
			if (admin.isTableEnabled(name))
				admin.disableTable(name);
			admin.deleteTable(name);
			System.out.println("Table deleted successfully");

		} catch (TableNotFoundException e) {
			// If table not found, print exception
			System.out.println("Table not found\n" + e.toString());
		}

		admin.close();
	}

}
