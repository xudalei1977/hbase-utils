import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

public class HBaseRowCounter {

    public static Configuration conf;//管理HBase的配置信息
    public static Connection conn;//管理HBase的连接
    public static Admin admin;//管理HBase数据库的连接

    public static void init(String host, String hbaseRootDir) throws IOException {
        conf = HBaseConfiguration.create();
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        conf.set("HADOOP_USER_NAME", "hadoop");
        conf.set("hbase.rootdir", hbaseRootDir);
        conf.set("hbase.zookeeper.quorum", host);//配置Zookeeper的ip地址
        conf.set("hbase.zookeeper.property.clientPort", "2181");//配置zookeeper的端口

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
            System.out.println(myTableName + "表已经存在");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String str : colFamily) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    public static void insertData(String tableName,String rowkey,String colFamily,String col,String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowkey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(),value.getBytes());
        table.put(put);
        table.close();
    }

    public static void deleteData(String tableName,String rowkey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowkey.getBytes());
        table.delete(delete);
        table.close();
    }

    public static void getData(String tableName,String rowkey,String colFamily,String col) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowkey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        System.out.println(new String(result.getValue(colFamily.getBytes(),col.getBytes())));
        table.close();
    }

    public static long rowCounter(String tableName, String startRow, String stopRow) throws IOException{
        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        ResultScanner rs = table.getScanner(scan);
        long rowCount = 0;
        for (Result result : rs) {
            //System.out.println("========= " + result.size());
            rowCount += result.size();
        }

        return rowCount;
    }

    public static long rowCounter(String tableName, String startRow, String stopRow,
                                  String filterColumn, String filterColumnValue,
                                  String aggColumnName) throws IOException{
        Table table = conn.getTable(TableName.valueOf(tableName));

        String filterColumnFamily = "";
        String filterColumnName = "";
        String[] arr1 = filterColumn.split(":");
        if (arr1.length == 2){
            filterColumnFamily = arr1[0];
            filterColumnName = arr1[1];
        }

        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(filterColumnFamily),
                Bytes.toBytes(filterColumnName),
                CompareOp.EQUAL,
                Bytes.toBytes(filterColumnValue));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        scan.setFilter(filter1);

        ResultScanner rs = table.getScanner(scan);
        long rowCount = 0;
        for (Result result : rs) {
            System.out.println("========= " + result.size());
            rowCount += result.size();
        }

        return rowCount;
    }

    /**
     * args 0: zookeeper host, e.g. 10.0.0.51
     * args 1: hbase data dir, e.g. s3://dalei-demo/hbase1
     * args 2: table name, e.g. usertable
     * args 3: hbase table start row, e.g. user1000000003865509391
     * args 4: hbase table stop row, e.g. user1000000005803208364
     * args 5: hbase table column name filter,, e.g. sf:c1
     * args 6: hbase table column value filter,, e.g. sku1
     * args 7: hbase table column value filter,, e.g. sf:c2
     * sample: java -jar hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar 10.0.0.68 s3://dalei-demo/hbase1 usertable user1000000001382941188 user1000000001382951188
     * sample: java -jar hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar 10.0.0.75 s3://dalei-demo/hbase1 test1 "user1|ts1" "user1|ts3" "sf:c1" sku1 "sf:c2"
     **/
    public static void main(String[] args) throws IOException {

        System.out.println("========= start." + new Date());
        init(args[0], args[1]);
        System.out.println("========= hbase connection is ok." + new Date());

        long rowCount = 0L;
        if (args.length == 5)
            rowCount = rowCounter(args[2], args[3], args[4]);
        else
            rowCount = rowCounter(args[2], args[3], args[4], args[5], args[6], args[7]);

        System.out.println("the row count is := " + rowCount);
        close();
    }
}
