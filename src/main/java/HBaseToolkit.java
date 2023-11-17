//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//
//public class HBaseToolkit {
//
//    public static boolean createTable(String tableName, String... familyNames) {
//        try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
//
//            TableName tName = TableName.valueOf(tableName);
//            if (admin.tableExists(tName)) {
//                return false;
//            }
//            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tName);
//            for (String familyName : familyNames) {
//                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName));
//            }
//            admin.createTable(builder.build());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return true;
//    }
//
//    public static boolean insertRow(String tableName, String rowKey, String columnFamilyName, String qualifer, String data) {
//        try (Table table = HBaseConn.getTable(tableName)) {
//            Put put = new Put(Bytes.toBytes(rowKey));
//            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifer), Bytes.toBytes(data));
//            table.put(put);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return true;
//    }
//
//    public static Result selectRow(String tableName, String rowKey) {
//        try (Table table = HBaseConn.getTable(tableName)) {
//            Get get = new Get(Bytes.toBytes(rowKey));
//            return table.get(get);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    public static void deleteRow(String tableName, String rowKey) {
//        try (Table table = HBaseConn.getTable(tableName)) {
//            Delete delete = new Delete(Bytes.toBytes(rowKey));
//            table.delete(delete);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//}
