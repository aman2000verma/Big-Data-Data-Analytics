package ddl;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * Create a table in HBase with 2 column families
 */
public class CreateTable {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get name of table to be created
		Scanner scan = new Scanner(System.in);
		System.out.println("Create Table\nEnter Table Name: ");
		String name = scan.nextLine();
		scan.close();

		// Creating table and column families instances
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(name));
		HColumnDescriptor fam1 = new HColumnDescriptor("colFam1");
		HColumnDescriptor fam2 = new HColumnDescriptor("colFam2");

		// Add column families in table
		table.addFamily(fam1);
		table.addFamily(fam2);

		// Create the table
		try{
		admin.createTable(table);
		System.out.println("Table Created");
		}
		catch(TableExistsException e){
			System.out.println("A table with this name already exist.\n" + e.toString());
		}
		admin.close();

	}

}
