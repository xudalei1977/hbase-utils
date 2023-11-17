//import org.apache.hadoop.hbase.client.Result;
//import java.io.IOException;
//
//public class HBaseJavaApiTest {
//    public static void  main(String[] args) throws IOException {
//        HBaseToolkit.createTable("wangcheng:student", "info", "score");
//
//        HBaseToolkit.insertRow("wangcheng:student", "001", "info", "name", "Tom");
//        HBaseToolkit.insertRow("wangcheng:student", "001", "info", "student_id", "20210000000001");
//        HBaseToolkit.insertRow("wangcheng:student", "001", "info", "class", "1");
//        HBaseToolkit.insertRow("wangcheng:student", "001", "score", "understanding", "75");
//        HBaseToolkit.insertRow("wangcheng:student", "001", "score", "programming", "82");
//        Result result1 = HBaseToolkit.selectRow("wangcheng:student", "001");
//        System.out.println(result1);
//
//        HBaseToolkit.insertRow("wangcheng:student", "002", "info", "name", "Jerry");
//        HBaseToolkit.insertRow("wangcheng:student", "002", "info", "student_id", "20210000000002");
//        HBaseToolkit.insertRow("wangcheng:student", "002", "info", "class", "1");
//        HBaseToolkit.insertRow("wangcheng:student", "002", "score", "understanding", "85");
//        HBaseToolkit.insertRow("wangcheng:student", "002", "score", "programming", "67");
//        Result result2 = HBaseToolkit.selectRow("wangcheng:student", "002");
//        System.out.println(result2);
//
//        HBaseToolkit.insertRow("wangcheng:student", "003", "info", "name", "Jack");
//        HBaseToolkit.insertRow("wangcheng:student", "003", "info", "student_id", "20210000000003");
//        HBaseToolkit.insertRow("wangcheng:student", "003", "info", "class", "2");
//        HBaseToolkit.insertRow("wangcheng:student", "003", "score", "understanding", "80");
//        HBaseToolkit.insertRow("wangcheng:student", "003", "score", "programming", "80");
//        Result result3 = HBaseToolkit.selectRow("wangcheng:student", "003");
//        System.out.println(result3);
//
//        HBaseToolkit.insertRow("wangcheng:student", "004", "info", "name", "Rose");
//        HBaseToolkit.insertRow("wangcheng:student", "004", "info", "student_id", "20210000000004");
//        HBaseToolkit.insertRow("wangcheng:student", "004", "info", "class", "2");
//        HBaseToolkit.insertRow("wangcheng:student", "004", "score", "understanding", "60");
//        HBaseToolkit.insertRow("wangcheng:student", "004", "score", "programming", "61");
//        Result result4 = HBaseToolkit.selectRow("wangcheng:student", "004");
//        System.out.println(result4);
//
//        HBaseToolkit.insertRow("wangcheng:student", "005", "info", "name", "wangcheng");
//        HBaseToolkit.insertRow("wangcheng:student", "005", "info", "student_id", "20210735010489");
//        HBaseToolkit.insertRow("wangcheng:student", "005", "info", "class", "3");
//        HBaseToolkit.insertRow("wangcheng:student", "005", "score", "understanding", "100");
//        HBaseToolkit.insertRow("wangcheng:student", "005", "score", "programming", "100");
//        Result result5 = HBaseToolkit.selectRow("wangcheng:student", "005");
//        System.out.println(result5);
//
//        HBaseToolkit.deleteRow("wangcheng:student", "002");
//        Result result6 = HBaseToolkit.selectRow("wangcheng:student", "002");
//        System.out.println(result6);
//
//        HBaseConn.closeConn();
//    }
//}