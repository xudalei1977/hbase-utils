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

public class HBaseAccess {

    public static Configuration conf;
    public static Connection conn;
    public static Admin admin;

    public static void init(String host, String hbaseRootDir) throws IOException {
        conf = HBaseConfiguration.create();
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        conf.set("HADOOP_USER_NAME", "hadoop");
        conf.set("hbase.rootdir", hbaseRootDir);
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
                //System.out.println("****** rowKey := " + rowKey);
                put = new Put(rowKey.getBytes());
                put.addColumn("cf_1".getBytes(), "field0".getBytes(), ("this is a demo value of " + rowKey).getBytes());
                put.addColumn("cf_1".getBytes(), "field1".getBytes(), ("this is a demo value of " + rowKey).getBytes());
                put.addColumn("cf_1".getBytes(), "field2".getBytes(), ("this is a demo value of " + rowKey).getBytes());
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
            //System.out.println("***** rs := " + rs[50].toString());
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
        init(host, hbaseRootDir);
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

    /**
     * function: put|get demo data to|from usertable, we use parameter to control the batch times, each batch we put|get 100 records.
     * args 0: zookeeper host, e.g. 10.0.0.51
     * args 1: hbase data dir, e.g. hdfs://10.0.0.51:8020/user/hbase
     * args 2: operation, e.g. put|get
     * args 3: repeat, execute operation times e.g. 20|50|100
     * args 4: startRegionOffset, start region e.g. any int from 1 to 50
     * sample: java -jar hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar 10.0.0.250 hdfs://10.0.0.51:8020/user/hbase put 10 1
     **/
    public static void main(String[] args) throws Exception {
        long start = (new Date()).getTime();
        init(args[0], args[1]);
        switch(args[2]) {
            case "put":
                putDemoData(Integer.valueOf(args[3]), Integer.valueOf(args[4]));
                break;
            case "get":
                getDemoData(Integer.valueOf(args[3]), Integer.valueOf(args[4]));
                break;
        }
        long end = (new Date()).getTime();
        System.out.println("========= time cost : " + Double.valueOf(end-start)/1000);
        close();
    }
}
