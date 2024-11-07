import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.CompareOperator.*;
import java.io.IOException;


public class HBaseRowCounter {

    public static Configuration conf;
    public static Connection conn;
    public static Admin admin;

    public static void init(String host) throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", host);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    public static void close() throws IOException {
        if (admin != null)
            admin.close();
        if (conn != null)
            conn.close();
    }

    public static long rowCounter(String tableName, String startRow, String stopRow) throws IOException{
        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        ResultScanner rs = table.getScanner(scan);
        long rowCount = 0;
        while (rs.next() != null)
            rowCount++;

//        for (Result result : rs) {
//            rowCount += result.size();
//        }

        return rowCount;
    }

    public static long rowCounter(String tableName, String startRow, String stopRow,
                                  String filterColumn, String filterColumnValue) throws IOException{
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
                CompareOperator.EQUAL,
                Bytes.toBytes(filterColumnValue));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));
        scan.setFilter(filter1);

        ResultScanner rs = table.getScanner(scan);
        long rowCount = 0;
        while (rs.next() != null)
            rowCount++;

        return rowCount;
    }

    /**
     * args 0: zookeeper host, e.g. 10.0.0.250
     * args 1: table name, e.g. usertable
     * args 2: hbase table start row, e.g. user1000
     * args 3: hbase table stop row, e.g. user5000
     * args 4: hbase table column name, e.g. family:field0
     * args 5: hbase table column value, e.g. value0
     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseRowCounter 10.0.0.250 usertable "user1000" "user5000" "family:field0" "value0"
     **/
    public static void main(String[] args) throws IOException {
        init(args[0]);
        long rowCount = 0L;
        rowCount = rowCounter(args[1], args[2], args[3], args[4], args[5]);
        System.out.println("the row count is := " + rowCount);
        close();
    }
}
