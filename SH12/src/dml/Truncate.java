package dml;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Truncate the table
 */
public class Truncate {

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

			Scan s = new Scan(Bytes.toBytes(1));

			ResultScanner scanner = hTable.getScanner(s);

			// Count the rows
			int count = 0;
			for (Result rs = scanner.next(); rs != null; rs = scanner.next()) {
				count++;
			}

			// Deleting by row
			for (int i = 0; i < count; i++) {
				Delete d = new Delete(Bytes.toBytes(i + 1));
				hTable.delete(d);
			}

			System.out.println("Table Truncated");

			hTable.close();
		} else
			System.out.println("Table does not exist");
		scan.close();
		admin.close();

	}
}
