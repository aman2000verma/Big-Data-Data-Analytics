package dml;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Delete data from table
 */
public class DeleteData {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Delete Data from Table\nEnter Table Name: ");
		String name = scan.nextLine();

		if (admin.tableExists(name)) {
			HTable hTable = new HTable(con, name);

			System.out
					.println("Enter data in following format:\nRow, Column Family Name, Column Name");
			String in = scan.nextLine();
			String[] tokens = in.split(",");

			// Row
			Delete d = new Delete(Bytes.toBytes(tokens[0].trim()));

			d.deleteColumn(Bytes.toBytes(tokens[1].trim()),
					Bytes.toBytes(tokens[2].trim()));

			hTable.delete(d);
			System.out.println("Column Deleted Successfully");

			hTable.close();
		} else
			System.out.println("Table does not exist");
		scan.close();
		admin.close();

	}
}
