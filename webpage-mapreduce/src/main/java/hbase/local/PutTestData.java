package hbase.local;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

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

        File file = new File("src/main/resources/data");
        BufferedReader reader = new BufferedReader(new FileReader(file));

        String line = "";
        int count = 0;
        while ((line = reader.readLine()) != null) {
            System.out.println(++count);
            String [] fields = line.split("\t");

            Put news = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
            news.addColumn(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER),
                    Bytes.toBytes(fields[2]));
            news.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(TITLE_QUALIFIER),
                    Bytes.toBytes(fields[0]));
            news.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(MAIN_TYPE_QUALIFIER),
                    Bytes.toBytes(fields[1]));
            news.addColumn(Bytes.toBytes(META_FAMILY), Bytes.toBytes(SUB_TYPE_QUALIFIER),
                    Bytes.toBytes(""));

            table.put(news);
        }

        table.close();
        connection.close();
    }
}
