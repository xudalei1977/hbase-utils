[toc]

### HBase Operation with Java API

#### 1. Program

1. HBaseAccess: put and get data on the HBase table, the demo table is 'usertable'
2. HBaseRowCounter: calculate the HBase table row number with scan and filter.

#### 2. Environment

```markdown
*  EMR 6.14.0 (hbase 2.4.17), 1 m6g.2xlarge primary code, 10 m6g.2xlarge core node.
```

#### 3. Build & Run

##### 3.1 Build

```properties
# mvn clean package -Dscope.type=provided 
```

##### 3.2 Demo table and data

We need to create the demo table usertalbe, and load data with YCSB.

```properties 
sudo su root
echo "n_splits = 2000; create 'usertable', {NAME =>'family', COMPRESSION => 'snappy'}, {SPLITS => (1..n_splits).map {|i| \"user#{10000+i*(99999-10000)/n_splits}\"}}" | hbase shell

cd /root
wget https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz -P /root
tar xfvz ycsb-0.17.0.tar.gz
mv ycsb-0.17.0 YCSB
mkdir -p /root/YCSB/conf
cp /etc/hbase/conf/hbase-site.xml /root/YCSB/conf/

cat << EOF > /root/YCSB/workloads/hbase_on_hdfs_write
recordcount=20000000
operationcount=20000000
workload=site.ycsb.workloads.CoreWorkload
fieldlength=200
fieldcount=100
readallfields=true
writeallfields=true
readproportion=0
updateproportion=0
scanproportion=0
insertproportion=1
requestdistribution=uniform
EOF

FILE=hbase_on_hdfs_write;
NOWSTR=$(date +%Y_%m_%d___%H_%M_%S);
T=64;
FIX=$FILE.thread.$T.$NOWSTR;
GCFILE=gc.log.$FIX;
nohup /root/YCSB/bin/ycsb load hbase20 -jvm-args "-Xmx20g -XX:+PrintGCDetails -Xloggc:$GCFILE" -P /root/YCSB/workloads/$FILE -cp /root/YCSB/conf/ -p table=usertable -p columnfamily=family -p hdrhistogram.percentiles=95,99,99.9 -s -threads $T > run.log.$FIX  2>&1 &
```

##### 3.3 Run the program

* HBaseAccess

```properties
* function: put|get demo data to|from usertable, we use parameter to control the batch times, each batch we put|get 100 records.
* args 0: zookeeper host, e.g. 10.0.0.51
* args 1: operation, e.g. put|get
* args 2: repeat, execute operation times e.g. 20|50|100
* args 3: startRegionOffset, e.g. 10|20|30|
* sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseAccess 10.0.0.250 put 100 20

```

* HBaseAccess

```properties
* function: check the row number in hbase table, with scan and filter.
* args 0: zookeeper host, e.g. 10.0.0.250
* args 1: table name, e.g. usertable
* args 2: hbase table start row, e.g. user1000
* args 3: hbase table stop row, e.g. user5000
* args 4: hbase table column name, e.g. family:field0
* args 5: hbase table column value, e.g. value0
* sample: java -classpath hbase-utils-1.0-SNAPSHOT-jar-with-dependencies.jar HBaseRowCounter 10.0.0.250 usertable "user1000" "user5000" "family:field0" "value0"
```
