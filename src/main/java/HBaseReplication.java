import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.regionserver.RegionReplicaReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;

import java.lang.Exception;
import java.util.*;

public class HBaseReplication {

    public static Configuration conf1;
    public static Configuration conf2;
    public static Connection conn1;
    public static Connection conn2;
    public static Admin admin1;
    public static Admin admin2;

    public static final String peerID = "peer_1";

    public static void init(String host1, String host2) throws Exception {
        conf1 = HBaseConfiguration.create();
        conf2 = HBaseConfiguration.create();

        conf1.set("hbase.zookeeper.quorum", host1);
        conf2.set("hbase.zookeeper.quorum", host2);
        conf1.set("hbase.zookeeper.property.clientPort", "2181");
        conf2.set("hbase.zookeeper.property.clientPort", "2181");

        conn1 = ConnectionFactory.createConnection(conf1);
        conn2 = ConnectionFactory.createConnection(conf2);

        admin1 = conn1.getAdmin();
        admin2 = conn2.getAdmin();
    }

    public static void close() throws Exception {
        if (admin1 != null)
            admin1.close();
        if (admin2 != null)
            admin2.close();
        if (conn1 != null)
            conn1.close();
        if (conn2 != null)
            conn2.close();
    }

    public static void checkAndCreateNamespace(String namespace) throws Exception {
        NamespaceDescriptor nsd = admin1.getNamespaceDescriptor(namespace);
        try {
            nsd = admin1.getNamespaceDescriptor(namespace);
        } catch (org.apache.hadoop.hbase.NamespaceNotFoundException e) {
            System.out.println("========= Namespace does not exist in source cluster, now exit.");
            return;
        }

        try {
            nsd = admin2.getNamespaceDescriptor(namespace);
            System.out.println("========= Namespace in target cluster." + nsd);
        } catch (org.apache.hadoop.hbase.NamespaceNotFoundException e) {
            System.out.println("========= Namespace does not exist in target cluster, now create it.");
            admin2.createNamespace(NamespaceDescriptor.create(namespace).build());
        }
    }

    public static void createNamespaceReplication(String namespace) throws Exception {
        checkAndCreateNamespace(namespace);

        TableName[] tableNames = admin1.listTableNamesByNamespace(namespace);
        for (TableName tableName : tableNames) {
            admin1.enableTableReplication(tableName);
        }
    }

    public static void createTableReplication(String namespace, String tableName) throws Exception {
        checkAndCreateNamespace(namespace);
        TableName _tableName = TableName.valueOf(namespace + ":" + tableName);
        admin1.enableTableReplication(_tableName);
    }

    public static void addPeer(String targetClusterIP) throws Exception {
        ReplicationPeerConfig rpc = null;

        try {
            rpc = admin1.getReplicationPeerConfig(peerID);
        } catch (org.apache.hadoop.hbase.ReplicationPeerNotFoundException e) {
            System.out.println("========= Peer does not exist, now create it.");
        }

        if(rpc == null) {
            admin1.addReplicationPeer(peerID,
                ReplicationPeerConfig.newBuilder()
                    .setClusterKey(String.format("%s:2181:/hbase", targetClusterIP))
                    .setSerial(true)
                    .build(),
                    true);
        }
    }

    public static void demo(String namespace, String tableName) throws Exception {
        admin1.createNamespace(NamespaceDescriptor.create(namespace).build());
        admin2.createNamespace(NamespaceDescriptor.create(namespace).build());

        TableName _tableName = TableName.valueOf(namespace + ":" +tableName);
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(_tableName);
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("f1")).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build());
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("f2")).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build());
        TableDescriptor tabA = builder.build();
        admin1.createTable(tabA);
        admin2.createTable(tabA);

        Table htab1A = conn1.getTable(_tableName);
        Table htab2A = conn2.getTable(_tableName);

        ReplicationPeerConfig rpc = admin1.getReplicationPeerConfig(peerID);
        admin1.updateReplicationPeerConfig(peerID,
                ReplicationPeerConfig.newBuilder(rpc).setReplicateAllUserTables(false).build());

        // add ns1 to peer config which replicate to cluster2
        rpc = admin1.getReplicationPeerConfig(peerID);
        Set<String> namespaces = new HashSet<>();
        namespaces.add(namespace);
        admin1.updateReplicationPeerConfig(peerID,
                ReplicationPeerConfig.newBuilder(rpc).setNamespaces(namespaces).build());
        System.out.println("update peer config");
    }

    /**
     * function: build the hbase replication for specific namespace and table
     * args 0: source hbase zookeeper host, e.g. 10.0.0.218
     * args 1: target hbase zookeeper host, e.g. 10.0.0.181
     * args 2: source namespace to be replicated e.g. test_ns
     * args 3: source table to be replicated, if not specified, replicate all table in the namespace e.g. test_1
     * sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseReplication 10.0.0.218 10.0.0.181 test_ns test_1
     **/
    public static void main(String[] args) throws Exception {

        if (args.length != 3 && args.length !=4){
            System.out.println("========= The command should like : java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseReplication 10.0.0.51 10.0.0.52 test_ns");
            System.exit(1);
        }

        long start = (new Date()).getTime();
        init(args[0], args[1]);

        //demo(args[3], args[4]);
        addPeer(args[1]);

        if (args.length == 3)
            createNamespaceReplication(args[2]);
        else
            createTableReplication(args[2],args[3]);

        long end = (new Date()).getTime();
        System.out.println("========= time cost : " + Double.valueOf(end-start)/1000);
        close();
    }
}
