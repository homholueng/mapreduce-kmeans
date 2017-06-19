package mapred;

import hbase.config.TableConfig;
import mapred.config.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import util.SegmentorFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

    static final String RESULT_1_PATH = "kmeans-mapred1";
    static final String RESULT_2_PATH = "kmeans-mapred2";
    static final String TFIDF_OUTPUT = "kmeans-mapred-tfidf-output";
    static final String VECTOR_BUILDER_OUTPUT = "kmeans-mapred-vector-builder-output";


    //5
    static final int K = 10;
    static final String INITIAL_CENTERS_OUTPUT = "/cit/output";

    //6
    static final String CENTERS_FILE_PATH = INITIAL_CENTERS_OUTPUT + "/part-r-00000";
    static final String FINAL_CENTERS_OUTPUT = "/km/output";
    static final int TOTAL_ITER = 5;

    //7
    static final String FINAL_CENTERS_FILE_PATH = FINAL_CENTERS_OUTPUT + "/part-r-00000";
    static final String FINAL_OUTPUT = "/final/output";



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
            ids.add(new String(bytesWritable.get()));

            // make segment on content
//            byte[] bytes = row.getValue(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER));
//            String content = new String(bytes);
//            seg.reset(content);
//            IWord word = null;
//            while ((word = seg.next()) != null) {
//                words.add(word.getValue());
//            }
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
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("mapreduce.job.jar", "target/webpage-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar");
        initProcessCache(configuration);
        FileSystem fs = FileSystem.get(configuration);

        // 1
        Job job1 = Job.getInstance(configuration);
        job1.setJobName("PageContentSegment");
        job1.setJarByClass(PageContentSegmentor.class);
        job1.setMapperClass(PageContentSegmentor.MapClass.class);
        job1.setReducerClass(PageContentSegmentor.Reduce.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(CONTENT_FAMILY));
        // good for map reduce job
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        Path out1 = new Path(RESULT_1_PATH);
        fs.delete(out1, true);

        TableMapReduceUtil.initTableMapperJob(
                TableConfig.NEWS_TABLE_NAME,
                scan,
                PageContentSegmentor.MapClass.class,
                Text.class,
                IntWritable.class,
                job1
        );

        SequenceFileOutputFormat.setOutputPath(job1, out1);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);


        job1.waitForCompletion(true);

        // 2

        Job job2 = Job.getInstance(configuration);
        job2.setJobName("WordPercentCount");
        job2.setJarByClass(WordPercentCounter.class);
        job2.setMapperClass(WordPercentCounter.MapClass.class);
        job2.setReducerClass(WordPercentCounter.Reduce.class);

        Path in2 = new Path(RESULT_1_PATH);
        Path out2 = new Path(RESULT_2_PATH);
        fs.delete(out2,true);

        SequenceFileInputFormat.addInputPath(job2, in2);
        SequenceFileOutputFormat.setOutputPath(job2, out2);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        job2.waitForCompletion(true);

        // 3
        configuration.setInt(PAGE_COUNT_NAME, configuration.getInt(PAGE_COUNT_NAME, 100000));
        configuration.set(WORDS_FILE_PATH_NAME, HDFS_PREFIX + WORDS_CACHE_PATH);

        Path inPath3 = new Path(RESULT_2_PATH);
        Path outPath3 = new Path(TFIDF_OUTPUT);

        if (fs.exists(outPath3)) {
            fs.delete(outPath3, true);
        }

        Job job3 = Job.getInstance(configuration);
        job3.setJobName("TFIDF");

        job3.setJarByClass(TfIdf.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);

        job3.setMapperClass(TfIdf.TFIDFMapper.class);
        job3.setReducerClass(TfIdf.TFIDFReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);

        SequenceFileInputFormat.addInputPath(job3, inPath3);
        SequenceFileOutputFormat.setOutputPath(job3, outPath3);

        job3.waitForCompletion(true);

        // 4
        configuration.set(WORDS_FILE_PATH_NAME, HDFS_PREFIX + WORDS_CACHE_PATH);
        Job job4 = Job.getInstance(configuration);
        job4.setJobName("Vector Builder");

        job4.setJarByClass(VectorBuilder.class);
        job4.setMapperClass(VectorBuilder.VectorBuilderMapper.class);
        job4.setReducerClass(VectorBuilder.VectorBuilderReducer.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);

        if (fs.exists(new Path(VECTOR_BUILDER_OUTPUT))) {
            fs.delete(new Path(VECTOR_BUILDER_OUTPUT), true);
        }

        SequenceFileInputFormat.setInputPaths(job4, new Path(TFIDF_OUTPUT));
        SequenceFileOutputFormat.setOutputPath(job4, new Path(VECTOR_BUILDER_OUTPUT));

        job4.waitForCompletion(true);

        //5
        String vectorFilePath = VECTOR_BUILDER_OUTPUT;
        String outputDir = INITIAL_CENTERS_OUTPUT;
        configuration.get(Constants.IDS_FILE_PATH_NAME, KmeansDriver.IDS_CACHE_PATH);
        configuration.set(CentersInitializer.K, String.valueOf(K));
        Job job = Job.getInstance(configuration, "CentersInitializer");
        job.setJarByClass(CentersInitializer.class);
        job.setMapperClass(CentersInitializer.MyMapper.class);
        job.setReducerClass(CentersInitializer.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        SequenceFileInputFormat.addInputPath(job, new Path(vectorFilePath));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        job.waitForCompletion(true);

        //6
        Kmeans.startWithConf(VECTOR_BUILDER_OUTPUT, CENTERS_FILE_PATH, FINAL_CENTERS_OUTPUT, TOTAL_ITER, configuration);

        //7
//        String centersFilePath = FINAL_CENTERS_FILE_PATH;
//        outputDir = "final/output";
//        KmeansOutputer.startWithConf(vectorFilePath, centersFilePath, outputDir, configuration);


        //8
        KmeansOutputer.startWithConf(VECTOR_BUILDER_OUTPUT, FINAL_CENTERS_FILE_PATH, FINAL_OUTPUT, configuration);

    }
}
