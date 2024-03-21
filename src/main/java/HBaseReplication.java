import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;

import java.io.IOException;
import java.util.*;

public class HBaseReplication {

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

    public static void createReplication(String namespace) throws IOException {
        TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
        for (TableName tableName : tableNames) {
            createReplication(namespace, tableName.getNameAsString());
        }
    }

    public static void createReplication(String namespace, String tableName) throws IOException {
        //TableName[] tableNames = admin.listTableNamesByNamespace(namespace);

    }


    public static void createNameSpace(String namespace) throws IOException {
        //TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
        admin.createNamespace(namespace);
    }

    private void addPeer(String id, int masterClusterNumber,
                         int slaveClusterNumber) throws Exception {
        ReplicationAdmin replicationAdmin = null;
        try {
            replicationAdmin = new ReplicationAdmin(
                    configurations[masterClusterNumber]);
            ReplicationPeerConfig rpc = new ReplicationPeerConfig();
            rpc.setClusterKey(utilities[slaveClusterNumber].getClusterKey());
            replicationAdmin.addPeer(id, rpc);
        } finally {
            close(replicationAdmin);
        }
    }

    /**
     * function: build the hbase replication for spe
     * args 0: source hbase zookeeper host, e.g. 10.0.0.51
     * args 1: hbase data dir, e.g. hdfs://10.0.0.51:8020/user/hbase
     * args 2: target hbase zookeeper host, e.g. 10.0.0.52
     * args 3: source namespace to be replicated e.g. test_ns
     * args 4: source namespace to be replicated, if not specified, replicate all table in the namespace e.g. test_1
     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseReplication 10.0.0.51 hdfs://10.0.0.51:8020/user/hbase 10.0.0.52 test_ns
     **/
    public static void main(String[] args) throws Exception {
        if (args.length != 4 && args.length !=5){
            System.out.println("========= The command should like : java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseReplication 10.0.0.51 hdfs://10.0.0.51:8020/user/hbase 10.0.0.52 test_ns");
            System.exit(1);
        }

        long start = (new Date()).getTime();
        init(args[0], args[1]);


        long end = (new Date()).getTime();
        System.out.println("========= time cost : " + Double.valueOf(end-start)/1000);
        close();
    }
}
