import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.math.BigInteger;


public class HBaseReversedScan {

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

    /** put one day data for a SN (16 digits) and a day (YYYYMMDD)
     *  the Row Key is : MD5(SN).substring(0,4) + SN + YYYYMMDDHHMI
     */
    public static void putDemoData(String SN, String Day) throws Exception {
        Table table = conn.getTable(TableName.valueOf("device_data_202413"));
        List<Put> putList = new ArrayList();
        Put put = null;
        for(int i=0; i<24; i++){
            for(int j=0; j<60; j++) {
                String rowKey = getMD5(SN).substring(0,4) + SN + Day + String.format("%02d", i) + String.format("%02d", j);
                System.out.println("========= rowKey : " + rowKey);
                put = new Put(rowKey.getBytes());
                for(int k=0; k<100; k++) {
                    put.addColumn("f".getBytes(), ("field_" + String.format("%03d", k)).getBytes(), ("this is a demo value of field_" + String.format("%03d", k)).getBytes());
                }
                putList.add(put);
            }
        }
        table.put(putList);
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
        Table table = conn.getTable(TableName.valueOf("device_data_202413"));

        Scan scan = new Scan();
        scan.withStartRow(startRow.getBytes(), true);
        scan.withStopRow(stopRow.getBytes(), true);

        // Get the ResultScanner
        ResultScanner scanner = table.getScanner(scan);
        ArrayList<String> list = new ArrayList<>();
        // Iterate over the results
        for (Result result : scanner) {
            list.add(Bytes.toString(result.getRow()));
            System.out.println("Row key: " + Bytes.toString(result.getRow()));
            // Process the result further as needed
        }
        table.close();
    }

    public static void scanReversedData(String startRow, String stopRow) throws Exception {
        Table table = conn.getTable(TableName.valueOf("device_data_202413"));

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
        Table table = conn.getTable(TableName.valueOf("device_data_202413"));

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

    /** startRow > stopRow */
    public static void getLastRecordByReverse(String startRow, String stopRow) throws Exception {
        Table table = conn.getTable(TableName.valueOf("device_data_202413"));

        Scan scan = new Scan();
        scan.setReversed(true);
        scan.withStartRow(startRow.getBytes(), true);
        scan.withStopRow(stopRow.getBytes(), true);

        // Get the first record from scanner.
        ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();
        System.out.println("Row key: " + Bytes.toString(result.getRow()));

        table.close();
    }

    /** startRow > stopRow */
    public static void getLastRecord(String startRow, String stopRow) throws Exception {
        Table table = conn.getTable(TableName.valueOf("device_data_202413"));

        Scan scan = new Scan();
        scan.addFamily("f".getBytes());

        String rowKey = null;
        String newStartRow = startRow;
        String newStopRow = startRow;

        while (rowKey == null && newStartRow.compareTo(stopRow) <= 0){
            newStopRow = newStartRow;
            newStartRow = moveAhead(newStopRow, 4, 10);
            scan.withStartRow(newStartRow.getBytes(), true);
            scan.withStopRow(newStopRow.getBytes(), true);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                rowKey = Bytes.toString(result.getRow());
            }
        }

        System.out.println("Row key: " + rowKey);
        table.close();
    }

    /** get the specific part from end of a string, e.g. the last 4 bit,
     * * move ahead the string to specific step. e.g. "aaaa" move ahead 10 is "aaa0". */
    public static String moveAhead(String str, int offset, int step) {
        String subStr = str.substring(str.length()-offset);
        BigInteger bigInt = new BigInteger(subStr, 16);
        BigInteger newSubStr = bigInt.subtract(BigInteger.valueOf(step));
        String result = str.substring(0, str.length()-offset) + newSubStr.toString(16);
        return result;
    }

    public static String getMD5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(input.getBytes());
            return convertByteArrayToHexString(messageDigest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static String convertByteArrayToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * function: put|get demo data to|from usertable, we use parameter to control the batch times, each batch we put|get 100 records.
     * args 0: zookeeper host, e.g. 10.0.0.51
     * args 1: operation, e.g. put|get
     * args 2: repeat, execute operation times e.g. 20|50|100
     * args 3: startRegionOffset, e.g. 10|20|30|
     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar GoodWe 10.0.0.99 put SN_1234567890123 20241107
     **/
    public static void main(String[] args) throws Exception {
        long start = (new Date()).getTime();
        init(args[0]);
        switch(args[1]) {
            case "put":
                putDemoData(args[2], args[3]);
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
            case "last_rec_rev":
                getLastRecordByReverse(args[2], args[3]);
                break;
            case "last_rec":
                getLastRecord(args[2], args[3]);
                break;
        }
        long end = (new Date()).getTime();
        System.out.println("========= time cost : " + Double.valueOf(end-start)/1000);
        close();


    }
}
