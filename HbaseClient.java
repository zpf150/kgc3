package hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.ArrayList;
import java.util.List;

public class HbaseClient {
    public static void main(String[] args) throws Exception {
        System.out.println(exits("test1"));
        createtable("test2","cf1","cf2","cf3");
    }

    public static Connection connection;
    //实列化hbaeConfiguration对象,并设置参数
    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.150");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //获取hbase链接对象ConnectionFactory
        try {
             connection = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


  //判断表格是否存在
  public static boolean exits(String table) throws Exception {
        //获取admin对象
      Admin admin = connection.getAdmin();
      boolean b = admin.tableExists(TableName.valueOf(table));
      return b;
  }


  //创建表格
    public static void createtable(String table,String... familys ) throws Exception {
        Admin admin = connection.getAdmin();
        //new 一个 HtableDescribe
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        //增加列簇
        for (String family : familys) {
            //需要new  HColumnDescriptor对象
            hTableDescriptor.addFamily(new HColumnDescriptor(family));
        }
        //这下就可以创建了
        admin.createTable(hTableDescriptor);
        System.out.println("创建成功");
    }


    //删除表
    public static  void drop(String table) throws Exception {
        Admin admin = connection.getAdmin();
        //如果表存在，先下线，在删除，不存在，则不删除
        if(admin.tableExists(TableName.valueOf(table))){
            admin.disableTable(TableName.valueOf(table));
            admin.deleteTable(TableName.valueOf(table));
        }else {
            System.out.println("对不起，表格不存在");
        }
    }


    //插入数据
    public static  void  addput(String tablename,String rowkey,String coulmnfamily,String coulmn,String value) throws Exception {
        //首先拿到table表
        Table table = connection.getTable(TableName.valueOf(tablename));
        //new put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        //往里边添加东西
        put.add(Bytes.toBytes(coulmnfamily), Bytes.toBytes(coulmn), Bytes.toBytes(value));
        //添加完之后，告诉table表格
        table.put(put);
        System.out.println("添加成功");
    }


    //删除一行数据
    public static void deleterow(String table,String row) throws Exception {
        //获取到表格后，在删除
        Table table1 = connection.getTable(TableName.valueOf(table));
        //拿到table后，告诉table删除那些
        Delete delete = new Delete(Bytes.toBytes(row));
        //删除
        table1.delete(delete);
        System.out.println("一行数据删除成功");
    }


    //删除多行
    public  static  void  deleteRows(String table,String... rowkeys) throws Exception {
        Table table1 = connection.getTable(TableName.valueOf(table));
        List<Delete> list = new ArrayList<>();
        //把要是删除的多行放入到一个list中
        for (String rowkey : rowkeys) {
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            list.add(delete);
        }
        table1.delete(list);
    }

    //scan  扫描
    public static  void  scan(String table) throws Exception {
        //获取链接
        Table table1 = connection.getTable(TableName.valueOf(table));
        //需要new scan对象
        Scan scan = new Scan();
        //循环：注意第一次拿到的是一个个的Cells单元
        ResultScanner results = table1.getScanner(scan);
        for (Result result : results) {
            //这里拿到的是
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //get 获取
    public static void get(String table) throws Exception {
        Table table1 = connection.getTable(TableName.valueOf(table));
        //new Get
        Get get = new Get(Bytes.toBytes(table));
       //传递进去
        Result result = table1.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    }
    }
}
