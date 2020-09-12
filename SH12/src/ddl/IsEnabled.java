package ddl;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Checks whether table is enabled
 */
public class IsEnabled {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Enabled Status\nEnter Table Name: ");
		String name = scan.nextLine();
		scan.close();

		try {
			if (admin.isTableEnabled(name))
				System.out.println("Enabled: TRUE");
			else
				System.out.println("Enabled: FALSE");

		} catch (TableNotFoundException e) {
			System.out.println("Table not found\n" + e.toString());
		}

		admin.close();
	}

}
