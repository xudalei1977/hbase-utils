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

public class HBaseGet {

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
     * args 0: zookeeper host, e.g. 10.0.0.51
     * args 1: hbase data dir, e.g. s3://dalei-demo/hbase1
     * args 2: table name, e.g. usertable
     * args 3: column family, e.g. cf_1
     * args 4: row key list number, e.g. 100
     * args 5: retrive column, e.g. field0
     * sample: java -jar hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar 10.0.0.82 s3://dalei-demo/hbase1 usertable cf_1 100 field0
     **/
    public static void main(String[] args) throws Exception {
        System.out.println("========= start." + new Date());
        init(args[0], args[1]);
        System.out.println("========= hbase connection is ok." + new Date());

        Random rand = new Random();
        long randLong = rand.nextLong();

        // Print random integers
        System.out.println("Random Integers: " + String.format("%020d", randLong));


        //Result[] rs = getRangeHashKey(args[0], args[1], args[2], args[3], Arrays.asList(args[4].split(";")), args[5].split(";"));
//        for(int i=0; i<rs.length; i++)
//            System.out.println("=========" + rs[i]);
//        String a = "user1000000003382941188;user1000000003382941189;user1000000003382941198";
//        String[] arr = a.split(";");
//        System.out.println("=========" + arr.length);
//        for(int i=0;i<arr.length;i++)
//            System.out.println("=========" + arr[i]);

        System.out.println("========= complete." + new Date());

        close();
    }
}
