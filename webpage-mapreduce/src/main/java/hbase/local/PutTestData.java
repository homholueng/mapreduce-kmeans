package hbase.local;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import sun.tools.jconsole.Tab;

import java.io.IOException;

import static hbase.config.TableConfig.*;

/**
 * Created by HL on 14/06/2017.
 */


public class PutTestData {

    public static void main(String[] args) throws IOException, DeserializationException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(NEWS_TABLE_NAME);

        if (!admin.tableExists(tableName)) {
            HTableDescriptor newTable = new HTableDescriptor(TableName.valueOf("news-7"));
            newTable.addFamily(new HColumnDescriptor(CONTENT_FAMILY));
            newTable.addFamily(new HColumnDescriptor(META_FAMILY));
            admin.createTable(newTable);
        }
        Table table = connection.getTable(tableName);

        Put news1 = new Put(Bytes.toBytes(00001));
        news1.addColumn(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER),
                Bytes.toBytes("E3 展前发布会上，微软神秘的“天蝎座”新型游戏机终于揭开神秘面纱——最终命名为 Xbox One X，" +
                        "2017 年 11 月 7 日发售，黑、白两色，定价 499 美元。微软此次发布会首次全程开启 4K 直播，从而为 Xbox One X " +
                        "及其 4K 画质造势。在一开场展示了微软旗下历代游戏的进化历程后，微软详细介绍了 Xbox One X  的命名，以及它的外观及性能。"));
        news1.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(TITLE_QUALIFIER),
                Bytes.toBytes("新一代全球性能最强主机来了，Xbox One X 会让你掏腰包吗？"));
        news1.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(MAIN_TYPE_QUALIFIER),
                Bytes.toBytes(1));
        news1.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(SUB_TYPE_QUALIFIER),
                Bytes.toBytes(2));

        Put news2 = new Put(Bytes.toBytes(00002));
        news2.addColumn(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER),
                Bytes.toBytes("因为一系列的负面事件，从性骚扰丑闻，到被 Google 母公司 Alphabet 旗下负责自动驾驶业务的 Waymo " +
                        "公司指控盗取商业机密等等，Uber CEO 崔维斯·卡兰尼克（Travis Kalanick）终于在董事会的强制要求下，开始无限期休假。" +
                        "卡兰尼克表示，他会离开公司一段时间，重新思考如何建设一家世界级公司。但在信里，卡兰尼克没有提及具体的休假时长，也没有表示何时能重回公司。"));
        news2.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(TITLE_QUALIFIER),
                Bytes.toBytes("CXO 全没了，Uber 今天成了一家“无人驾驶”的公司"));
        news2.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(MAIN_TYPE_QUALIFIER),
                Bytes.toBytes(2));
        news2.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(SUB_TYPE_QUALIFIER),
                Bytes.toBytes(3));

        table.put(news1);
        table.put(news2);

        table.close();
        connection.close();
    }
}
