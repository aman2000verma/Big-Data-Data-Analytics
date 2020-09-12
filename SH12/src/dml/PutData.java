package dml;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Put data in table
 */
public class PutData {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Put Data in Table\nEnter Table Name: ");
		String name = scan.nextLine();

		if (admin.tableExists(name)) {
			HTable hTable = new HTable(con, name);

			System.out
					.println("Enter data in following format:\nRow, Column Family Name, Column Name, Value");
			String in = scan.nextLine();
			String[] tokens = in.split(",");

			// Row
			Put p = new Put(Bytes.toBytes(tokens[0].trim()));

			// Adding Data in put instance
			p.add(Bytes.toBytes(tokens[1].trim()),
					Bytes.toBytes(tokens[2].trim()),
					Bytes.toBytes(tokens[3].trim()));

			// Adding data in table
			hTable.put(p);
			System.out.println("Data inserted");

			hTable.close();
		} else
			System.out.println("Table does not exist");
		scan.close();
		admin.close();

	}
}
