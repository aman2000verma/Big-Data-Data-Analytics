package dml;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Get data from table
 */
public class GetData {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table
		Scanner scan = new Scanner(System.in);
		System.out.println("Get Data from Table\nEnter Table Name: ");
		String name = scan.nextLine();

		if (admin.tableExists(name)) {
			HTable hTable = new HTable(con, name);

			System.out
					.println("Enter data in following format:\nRow, Column Family Name, Column Name");
			String in = scan.nextLine();
			String[] tokens = in.split(",");

			// Row
			Get g = new Get(Bytes.toBytes(tokens[0].trim()));

			// Reading the data
			Result result = hTable.get(g);

			// Reading values from Result class object
			byte[] val = result.getValue(Bytes.toBytes(tokens[1].trim()),
					Bytes.toBytes(tokens[2].trim()));

			System.out.println("Value = " + Bytes.toString(val));

			hTable.close();
		} else
			System.out.println("Table does not exist");
		scan.close();
		admin.close();

	}
}
