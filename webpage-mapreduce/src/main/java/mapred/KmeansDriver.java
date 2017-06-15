package mapred;

import hbase.config.TableConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.lionsoul.jcseg.tokenizer.core.IWord;
import util.SegmentorFactory;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

import static hbase.config.TableConfig.*;
import static mapred.config.Constants.*;

/**
 * Created by HL on 15/06/2017.
 */
public class KmeansDriver {

    static final String WORDS_CACHE_NAME = "words.txt";
    static final String IDS_CACHE_NAME = "ids.txt";
    static final String WORDS_CACHE_PATH = "/tmp/" + WORDS_CACHE_NAME;
    static final String IDS_CACHE_PATH = "/tmp/" + IDS_CACHE_NAME;

    public static void initProcessCache(Configuration configuration) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(NEWS_TABLE_NAME));
        ResultScanner scanner = table.getScanner(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER));

        SegmentorFactory.Segmentor seg = SegmentorFactory.newInstance();
        Result row = null;

        // 所有单词
        Set<String> words = new HashSet<String>();
        // 所有 ID
        Set<String> ids = new HashSet<String>();
        // 网页数
        int pageCount = 0;
        while ((row = scanner.next()) != null) {
            // add id to set
            ImmutableBytesWritable bytesWritable = new ImmutableBytesWritable(row.getRow());
            ids.add(bytesWritable.toString().replaceAll(" ", ""));

            // make segment on content
            byte[] bytes = row.getValue(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER));
            String content = new String(bytes);
            seg.reset(content);
            IWord word = null;
            while ((word = seg.next()) != null) {
                words.add(word.getValue());
            }
            ++pageCount;
        }

        // set properties
        configuration.setInt(PAGE_COUNT_NAME, pageCount);
        configuration.set(WORDS_FILE_PATH_NAME, HDFS_PREFIX + WORDS_CACHE_PATH);
        configuration.set(IDS_FILE_PATH_NAME, HDFS_PREFIX + IDS_CACHE_PATH);

        // copy file to hdfs
        createFileAndWriteSet(WORDS_CACHE_NAME, words);
        createFileAndWriteSet(IDS_CACHE_NAME, ids);

        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(new Path(WORDS_CACHE_NAME), new Path(WORDS_CACHE_PATH));
        fileSystem.copyFromLocalFile(new Path(IDS_CACHE_NAME), new Path(IDS_CACHE_PATH));
    }

    public static void createFileAndWriteSet(String fileName, Set<?> set) throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            file.createNewFile();
        }

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
        for (Object i : set) {
            bufferedWriter.write(i.toString() + "\n");
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        initProcessCache(configuration);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(CONTENT_FAMILY));
        scan.setCaching(500);
        scan.setCacheBlocks(false);
    }
}
