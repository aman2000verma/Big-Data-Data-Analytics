package ddl;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Drops all table matching regex
 */
public class DropAllTables {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Drop Tables\nEnter Table Regex: ");
		String regex = scan.nextLine();
		scan.close();

		// Disable and drop the tables matching the regular expression
		try {
			for (String name : admin.getTableNames(regex)) {
				if (admin.isTableEnabled(name))
					admin.disableTable(name);
				admin.deleteTable(name);
				System.out.println(name + " dropped successfully");
			}

		} catch (TableNotFoundException e) {
			// If table not found, print exception
			System.out.println("Table not found\n" + e.toString());
		}

		admin.close();
	}

}
