package hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseClient2 {
    public static void main(String[] args) {

    }
    //全局的链接对象
    public static Connection connection;
    //静态代码块
    static {
        //获取conf
        HBaseConfiguration conf = new HBaseConfiguration();
        //设置参数
        conf.set("hbase.zookeeper.quorum", "192.168.1.150");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //获取链接对象
        try {
             connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//判断表是否存在
    public static boolean exits(String table) throws Exception {
        //拿到admin对象
        Admin admin = connection.getAdmin();
        //判断
        boolean b = admin.tableExists(TableName.valueOf(table));
        return b;
    }
    //创建表
    public static void createtable(String table,String...cf) throws Exception {
        Admin admin = connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        //遍历cf
        for (String s : cf) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        //创建
        admin.createTable(hTableDescriptor);
        System.out.println("创建成功");
    }
  //删除表
  public static void delete(String table) throws Exception {
      Admin admin = connection.getAdmin();
      admin.disableTable(TableName.valueOf(table));
      admin.deleteTable(TableName.valueOf(table));
      System.out.println("删除成功");
  }
  //添加数据
  public static void addtable(String table,String rowkey,String columnfamily,String column,String value) throws Exception {
      Table table1 = connection.getTable(TableName.valueOf(table));
      //new  Put
      Put put = new Put(Bytes.toBytes(rowkey));
      //接下来添加了
      put.add(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(value));
      //接下来告知上边，我有值了
      table1.put(put);
      System.out.println("添加成功");
  }
  //scan
    public  static  void show(String table) throws Exception {
        Table table1 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner results = table1.getScanner(scan);
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(cell.getFamilyArray().toString());
                System.out.println(cell.getRow().toString());
                System.out.println(cell.getQualifier().toString());
                System.out.println(cell.getValue().toString());
            }
        }
    }
    //get
    public static void get(String table) throws IOException {
        Table table1 = connection.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(table));
        Result result = table1.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(cell.getFamilyArray().toString());
            System.out.println(cell.getRow().toString());
            System.out.println(cell.getQualifier().toString());
            System.out.println(cell.getValue().toString());
        }
    }
}
