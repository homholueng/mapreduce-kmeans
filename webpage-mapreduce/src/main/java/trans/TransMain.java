package trans;

import hbase.config.TableConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import trans.mapper.NewsMapper;
import trans.model.News;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by HL on 20/06/2017.
 */
public class TransMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String resource = "mybatis/myBatis.xml";
        Class.forName("com.mysql.jdbc.Driver");
        InputStream inputStream = Resources.getResourceAsStream(resource);
        SqlSessionFactory factory = new SqlSessionFactoryBuilder().build(inputStream);
        SqlSession session = factory.openSession();
        NewsMapper mapper = session.getMapper(NewsMapper.class);

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table  = connection.getTable(TableName.valueOf(TableConfig.NEWS_TABLE_NAME));

        BufferedReader reader = new BufferedReader(new FileReader("src/main/resources/result.txt"));
        String line = null;
        byte[] metaFamily = Bytes.toBytes(TableConfig.META_FAMILY);
        int count = 0;
        while ((line = reader.readLine()) != null) {
            System.out.println(++count);
            String[] fields = line.split("\t");
            Get get = new Get(Bytes.toBytes(fields[1]));
            Result result = table.get(get);

            News news = new News();
            news.setId(fields[1]);
            news.setTypeId(Integer.parseInt(fields[0]));
            news.setUrl(new String(result.getValue(metaFamily, Bytes.toBytes(TableConfig.URL_QUALIFIER))));
            news.setTitle(new String(result.getValue(metaFamily, Bytes.toBytes(TableConfig.TITLE_QUALIFIER))));
            news.setMainType(new String(result.getValue(metaFamily, Bytes.toBytes(TableConfig.MAIN_TYPE_QUALIFIER))));
            news.setSubType(new String(result.getValue(metaFamily, Bytes.toBytes(TableConfig.SUB_TYPE_QUALIFIER))));
            mapper.insertNews(news);
        }

        session.commit();
        session.close();
        table.close();
        connection.close();
    }
}
