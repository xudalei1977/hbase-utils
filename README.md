# 使用Java API操作HBase
实践主要是建表，插入数据，删除数据，查询等功能。建立一个如下所示的表：

表名 $your_name:student

空白处自行填写, 姓名学号一律填写真实姓名和学号

![table](https://github.com/hugecheng/java-access-hbase/blob/master/table.jpg)

服务器版本为2.1.0（hbase版本和服务器上的版本可以不一致，但尽量保持一致）

```
<dependency>
  <groupId>org.apache.hbase</groupId>
  <artifactId>hbase-client</artifactId>
  <version>2.1.0</version>
</dependency>
```
