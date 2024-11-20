import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.*;
import java.util.List;


public class HBaseAccess {

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

    public static void putDemoData(int repeat, int startRegionOffset) throws Exception {
        Table table = conn.getTable(TableName.valueOf("usertable"));
        for(int i=1; i<=repeat; i++){
            List<Put> putList = new ArrayList();
            Put put = null;
            for(int j=startRegionOffset; j<5000; j=j+50) {
                long num = i * 10010010010010L;
                String rowKey = "user" + String.format("%04d", j) + String.format("%015d", num);
                //System.out.println("========= rowKey : " + rowKey);
                put = new Put(rowKey.getBytes());
                put.addColumn("family".getBytes(), "field0".getBytes(), ("value0").getBytes());
                put.addColumn("family".getBytes(), "field1".getBytes(), ("this is a demo value of " + rowKey).getBytes());
                put.addColumn("family".getBytes(), "field2".getBytes(), ("this is a demo value of " + rowKey).getBytes());
                putList.add(put);
            }
            table.put(putList);
        }
        table.close();
    }

    public static void getDemoData(int repeat, int startRegionOffset) throws Exception {
        Table table = conn.getTable(TableName.valueOf("usertable"));

        for(int i=1; i<=repeat; i++){
            List<Get> getList = new ArrayList();
            Get get = null;
            for(int j=startRegionOffset; j<5000; j=j+50) {
                long num = i * 10010010010010L;
                String rowKey = "user" + String.format("%04d", j) + String.format("%015d", num);
                get = new Get(rowKey.getBytes());
                getList.add(get);
            }
            Result[] rs = table.get(getList);
        }
        table.close();
    }

    public static void scanDemoData(String startRow, String stopRow) throws Exception {
        Table table = conn.getTable(TableName.valueOf("usertable"));

        Scan scan = new Scan();
        scan.withStartRow(startRow.getBytes(), true);
        scan.withStopRow(stopRow.getBytes(), true);

        // Get the ResultScanner
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<String> list = new ArrayList<>();
        // Iterate over the results
        for (Result result : scanner) {
            list.add(Bytes.toString(result.getRow()));
            // Process the result further as needed
        }
        table.close();
    }

    public static void scanReversedData(String startRow, String stopRow) throws Exception {
        Table table = conn.getTable(TableName.valueOf("usertable"));

        Scan scan = new Scan();
        scan.setReversed(true);
        scan.withStartRow(startRow.getBytes(), true);
        scan.withStopRow(stopRow.getBytes(), true);

        // Get the ResultScanner
        ResultScanner scanner = table.getScanner(scan);

        // Iterate over the results
        for (Result result : scanner) {
            System.out.println("Row key: " + Bytes.toString(result.getRow()));
            // Process the result further as needed
        }
        table.close();
    }

    public static void scanAndReverseData(String startRow, String stopRow) throws Exception {
        Table table = conn.getTable(TableName.valueOf("usertable"));

        Scan scan = new Scan();
        scan.withStartRow(startRow.getBytes(), true);
        scan.withStopRow(stopRow.getBytes(), true);

        // Get the ResultScanner
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<String> list = new ArrayList<>();
        // Iterate over the results
        for (Result result : scanner) {
            list.add(Bytes.toString(result.getRow()));
            // Process the result further as needed
        }
        Collections.reverse(list);
        System.out.println("Reversed ArrayList: " + list);
        table.close();
    }

    /**
     * function: put|get demo data to|from usertable, we use parameter to control the batch times, each batch we put|get 100 records.
     * args 0: zookeeper host, e.g. 10.0.0.51
     * args 1: operation, e.g. put|get
     * args 2: repeat, execute operation times e.g. 20|50|100
     * args 3: startRegionOffset, e.g. 10|20|30|
     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseAccess 10.0.0.250 put 20 10
     **/
    public static void main(String[] args) throws Exception {
        long start = (new Date()).getTime();
        init(args[0]);
        switch(args[1]) {
            case "put":
                putDemoData(Integer.valueOf(args[2]), Integer.valueOf(args[3]));
                break;
            case "get":
                getDemoData(Integer.valueOf(args[2]), Integer.valueOf(args[3]));
                break;
            case "scan":
                scanDemoData(args[2], args[3]);
                break;
            case "scan_rev":
                scanReversedData(args[2], args[3]);
                break;
            case "rev":
                scanAndReverseData(args[2], args[3]);
                break;
        }
        long end = (new Date()).getTime();
        System.out.println("========= time cost : " + Double.valueOf(end-start)/1000);
        close();
    }
}
