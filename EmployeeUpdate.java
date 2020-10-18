import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class UpdateMyFirstHbaseTable
{

	private static final String TABLE_NAME = "employees";
	private static final String CF_DEFAULT = "Personal Data";

	public static void main(String... args) throws IOException
	{

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));	
			
		    Get g = new Get(Bytes.toBytes("3"));
		    Table t = connection.getTable(table.getTableName());
			Result r = t.get(g);
			
			byte[] d = r.getValue(Bytes.toBytes("Professional Data"), Bytes.toBytes("Designation"));
			byte[] s = r.getValue(Bytes.toBytes("Professional Data"), Bytes.toBytes("salary"));
			
			Double salary = Double.parseDouble(Bytes.toString(s)); 
			salary = salary * 105 / 100;
			byte[] s2 = Bytes.toBytes(salary.toString());
			
			Put p = new Put(Bytes.toBytes("3"));
			p.addColumn(Bytes.toBytes("Professional Data"), Bytes.toBytes("Designation"), Bytes.toBytes("Sr. Engineer"));
			p.addColumn(Bytes.toBytes("Professional Data"), Bytes.toBytes("salary"), s2);
			
			t.put(p);
		}
	}
}