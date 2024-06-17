//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//
//import java.io.IOException;
//import java.util.Date;
//
//
//public class HBaseConn {
//    private static final HBaseConn INSTANCE = new HBaseConn();
//    private static Configuration configuration;
//    private static Connection connection;
//
//    private HBaseConn() {
//        try {
//            if (configuration == null) {
//                configuration = HBaseConfiguration.create();
//                configuration.set("hbase.zookeeper.quorum", "jikehadoop01:2181,jikehadoop02:2181,jikehadoop03:2181");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private Connection getConnection() {
//        if (connection == null || connection.isClosed()) {
//            try {
//                connection = ConnectionFactory.createConnection(configuration);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        return connection;
//    }
//
//    public static Connection getHBaseConn() {
//        return INSTANCE.getConnection();
//    }
//
//    public static Table getTable(String tableName) throws IOException {
//        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
//    }
//
//    public static void createTable(String myTableName, String[] colFamily) throws IOException {
//        TableName tableName = TableName.valueOf(myTableName);
//        if (admin.tableExists(tableName)) {
//            System.out.println(myTableName + "表已经存在");
//        } else {
//            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
//            for (String str : colFamily) {
//                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
//                hTableDescriptor.addFamily(hColumnDescriptor);
//            }
//            admin.createTable(hTableDescriptor);
//        }
//    }
////
////    public static void insertData(String tableName,String rowkey,String colFamily,String col,String value) throws IOException {
////        Table table = conn.getTable(TableName.valueOf(tableName));
////        Put put = new Put(rowkey.getBytes());
////        put.addColumn(colFamily.getBytes(),col.getBytes(),value.getBytes());
////        table.put(put);
////        table.close();
////    }
////
////    public static void deleteData(String tableName,String rowkey) throws IOException {
////        Table table = conn.getTable(TableName.valueOf(tableName));
////        Delete delete = new Delete(rowkey.getBytes());
////        table.delete(delete);
////        table.close();
////    }
////
////    public static void getData(String tableName,String rowkey,String colFamily,String col) throws IOException {
////        Table table = conn.getTable(TableName.valueOf(tableName));
////        Get get = new Get(rowkey.getBytes());
////        get.addColumn(colFamily.getBytes(),col.getBytes());
////        Result result = table.get(get);
////        System.out.println(new String(result.getValue(colFamily.getBytes(),col.getBytes())));
////        table.close();
////    }
//
//    public static void closeConn() {
//        if (connection != null) {
//            try {
//                connection.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    /**
//     * function: put|get demo data to|from usertable, we use parameter to control the batch times, each batch we put|get 100 records.
//     * args 0: zookeeper host, e.g. 10.0.0.51
//     * args 1: hbase data dir, e.g. hdfs://10.0.0.51:8020/user/hbase
//     * args 2: operation, e.g. put|get
//     * args 3: repeat, execute operation times e.g. 20|50|100
//     * args 4: startRegionOffset, start region e.g. any int from 1 to 50
//     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseAccess 10.0.0.250 put 20 100
//     **/
//    public static void main(String[] args) throws Exception {
//        long start = (new Date()).getTime();
////        init(args[0], args[1]);
////        switch(args[2]) {
////            case "put":
////                putDemoData(Integer.valueOf(args[3]), Integer.valueOf(args[4]));
////                break;
////            case "get":
////                getDemoData(Integer.valueOf(args[3]), Integer.valueOf(args[4]));
////                break;
////        }
////        long end = (new Date()).getTime();
////        System.out.println("========= time cost : " + Double.valueOf(end-start)/1000);
////        close();
//    }
//}
