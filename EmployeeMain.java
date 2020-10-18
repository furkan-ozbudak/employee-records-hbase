import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable
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
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
			table.addFamily(new HColumnDescriptor("Professional Data"));			

			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);

			System.out.println(" Done!");
			
			byte[] cfPersonal = Bytes.toBytes(CF_DEFAULT);
			byte[] cfProfessional = Bytes.toBytes("Professional Data");
			
			byte[] cName = Bytes.toBytes("Name");
			byte[] cCity = Bytes.toBytes("City");
			byte[] cDesignation = Bytes.toBytes("Designation");
			byte[] cSalary = Bytes.toBytes("salary");
			
			Put p = new Put(Bytes.toBytes("1"));
			p.addColumn(cfPersonal, cName, Bytes.toBytes("John"));
			p.addColumn(cfPersonal, cCity, Bytes.toBytes("Boston"));
			p.addColumn(cfProfessional, cDesignation, Bytes.toBytes("Manager"));
			p.addColumn(cfProfessional, cSalary, Bytes.toBytes("150000"));
			connection.getTable(table.getTableName()).put(p);
			
			p = new Put(Bytes.toBytes("2"));
			p.addColumn(cfPersonal, cName, Bytes.toBytes("Mary"));
			p.addColumn(cfPersonal, cCity, Bytes.toBytes("New York"));
			p.addColumn(cfProfessional, cDesignation, Bytes.toBytes("Sr. Engineer"));
			p.addColumn(cfProfessional, cSalary, Bytes.toBytes("130000"));
			connection.getTable(table.getTableName()).put(p);
			
			p = new Put(Bytes.toBytes("3"));
			p.addColumn(cfPersonal, cName, Bytes.toBytes("Bob"));
			p.addColumn(cfPersonal, cCity, Bytes.toBytes("Fremont"));
			p.addColumn(cfProfessional, cDesignation, Bytes.toBytes("Jr. Engineer"));
			p.addColumn(cfProfessional, cSalary, Bytes.toBytes("90000"));
			connection.getTable(table.getTableName()).put(p);
		}
	}
}