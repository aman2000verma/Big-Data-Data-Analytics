package ddl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * List all the User Tables
 */
public class ListTables {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

		// Creating HBase Admin instance
		Configuration con = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(con);

		// Get list of Table Names from HBase Admin
		for (TableName name : admin.listTableNames()) {
			System.out.println(name.toString());
		}

		admin.close();

	}

}
