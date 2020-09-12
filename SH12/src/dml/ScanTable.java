package dml;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Scan data from table
 */
public class ScanTable {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Scan Data from Table\nEnter Table Name: ");
		String name = scan.nextLine();

		if (admin.tableExists(name)) {
			HTable hTable = new HTable(con, name);

			System.out
					.println("Enter column families to scan (separted by commas)");

			String[] families = scan.nextLine().split(",");

			try {
				// Instantiating the Scan class
				Scan s = new Scan();
				for (String family : families)
					s.addFamily(Bytes.toBytes(family));

				ResultScanner scanner = hTable.getScanner(s);

				// Reading values from scan result
				for (Result result = scanner.next(); result != null; result = scanner
						.next())
					System.out.println("Row : " + result);
			} catch (NoSuchColumnFamilyException e) {
				System.out.println("No such column family found\n"
						+ e.toString());
			}

			hTable.close();
		} else
			System.out.println("Table does not exist");
		scan.close();
		admin.close();

	}
}
