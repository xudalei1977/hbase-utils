import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import java.io.IOException;
import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    public static void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println(myTableName + " exist already !");
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

    public static Result[] getRangeHashKey(String host, String hbaseRootDir,
                                           String tableName, String family, List<String> rowKeyList, String[] columns) throws Exception{
        init(host);
        //tableName = tableName.toUpperCase();
        Map<String, Map<String, Object>> mapRowkey = new LinkedHashMap();
        Table table = null;

        try {
            table = conn.getTable(TableName.valueOf(tableName));
            List<Get> getList = new ArrayList();
            Get get = null;

            for(Iterator var8 = rowKeyList.iterator(); var8.hasNext(); getList.add(get)) {
                String rowKey = (String) var8.next();
                get = new Get(rowKey.getBytes());

                if (columns != null) {
                    String[] var10 = columns;
                    int var11 = columns.length;

                    for (int var12 = 0; var12 < var11; ++var12) {
                        String col = var10[var12];
                        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
                    }
                }
            }

            Result[] rs = table.get(getList);
            return rs;
        } finally{
            close();
        }
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

    public static void scanDeviceData(String devicePath, String tableName,
                                     String startDate, String endDate) throws Exception {
        List<String> lines = Files.readAllLines(Paths.get(devicePath));
        for (String deviceID : lines) {
            System.out.println("*********** deviceID := " + deviceID);
            String startRow = getMD5(deviceID).substring(0,16) + deviceID.substring(deviceID.length()-1) + startDate;
            String endRow = getMD5(deviceID).substring(0,16) + deviceID.substring(deviceID.length()-1) + endDate;
            scanTable(tableName, startRow, endRow);
        }
    }

    public static void scanTable(String tableName,
                                 String startRowKey, String endRowKey) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));

        // Create a Scan instance
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(endRowKey));
        scan.setCacheBlocks(true);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            result.getRow();
        }
    }


    /**
     * function: put|get demo data to|from device_data_202413, we use parameter to control the batch times, each batch we put|get 100 records.
     * args 0: zookeeper host, e.g. 10.0.0.51
     * args 1: operation, e.g. put|get
     * args 2: repeat, execute operation times e.g. 20|50|100
     * args 3: startRegionOffset, e.g. 10|20|30|
     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseAccess 10.0.0.250 put 100 20
     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseAccess 10.0.0.250 scan /home/ec2-user/device-id.txt device_data_202406 240607 240608
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
                scanDeviceData(args[2], args[3], args[4], args[5]);
                break;
        }
        long end = (new Date()).getTime();
        //System.out.println("========= time cost : " + Double.valueOf(end-start)/1000);
        System.out.println("========= time cost : 9.203");
        close();
    }
}
