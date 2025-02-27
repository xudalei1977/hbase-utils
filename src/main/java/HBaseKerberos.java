import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class HBaseKerberos {

    public static Configuration conf;
    public static Connection conn;
    public static Admin admin;

    public static void init(String host, String principal, String keytab) throws IOException {
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        //System.setProperty("sun.security.krb5.debug", "true");
        //System.setProperty("java.security.auth.login.config", "/etc/jaas.conf");

        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("hbase.rpc.timeout", "120000");
        conf.set("hbase.client.scanner.timeout.period", "120000");

        //conf.set("hbase.cluster.distributed", "true");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@mycompany.com");
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@mycompany.com");

//        conf.set("hbase.client.retries.number", "3");
//        conf.set("hbase.client.pause", "500");
//        conf.set("zookeeper.recovery.retry",  "1");

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public static void close() throws IOException {
        if (admin != null)
            admin.close();
        if (conn != null)
            conn.close();
    }

    public static void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println(myTableName + " : the table exist already !");
        } else {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            tableDescriptorBuilder.setMaxFileSize(HConstants.FOREVER); // Set the maximum file size to unlimited
            for (String str : colFamily) {
                ColumnFamilyDescriptorBuilder columnFamily1Builder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str));
                tableDescriptorBuilder.setColumnFamily(columnFamily1Builder.build());
            }
            admin.createTable(tableDescriptorBuilder.build());
        }
    }

    public static void insertData(String tableName, String rowKey, String colFamily, String column, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), column.getBytes(), (value).getBytes());
        table.put(put);
        table.close();
    }


    public static void getData(String tableName, String rowKey, String colFamily, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(), column.getBytes());
        Result result = table.get(get);
        System.out.println("the column value is : " + new String(result.getValue(colFamily.getBytes(), column.getBytes())));
        table.close();
    }


    /**
     * function: test hbase api with kerberos.
     * args 0: zookeeper host, e.g. 10.0.0.51
     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseKerberos 10.0.0.228 hbase/ip-10-0-0-228.ec2.internal@EC2.INTERNAL /etc/hbase.keytab
     **/
    public static void main(String[] args) throws Exception {
        long start = (new Date()).getTime();

        init(args[0], args[1], args[2]);
        createTable("test2", new String[] {"sf"});
        insertData("test2", "rowkey1", "sf", "name", "daleixu");
        getData("test1", "user1|ts21", "sf", "c1");
        close();

        long end = (new Date()).getTime();
        System.out.println("========= time cost : " + Double.valueOf(end-start)/1000);
    }
}
