import mapred.CentersInitializer;
import mapred.KmeansDriver;
import mapred.config.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Created by yuan on 17-6-15.
 */
public class CentersInitializerTest {


    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private Text docID;
        private Text content = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            docID = value;
            String vector = createVector();
            content.set(vector.toString());
            context.write(docID, content);
        }

        /**
         * 产生随机向量
         * @return
         */
        private String createVector() {
            StringBuilder sb = new StringBuilder();
            double v;
            v = Math.random();
            sb.append(v);
            for (int i = 1; i < 4; i++) {
                v = Math.random();
                sb.append("&");
                sb.append(v);
            }
            return sb.toString();
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    /**
     * 用于创建测试数据文件
     */
    @Test
    public void creatTestData() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        KmeansDriver.initProcessCache(conf);

        Job job = Job.getInstance(conf);
        job.setJarByClass(CentersInitializerTest.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path("/cit/input/01"));
//        TextOutputFormat.setOutputPath(job, new Path("/cit/output"));

        SequenceFileOutputFormat.setOutputPath(job, new Path("/cit/output"));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path("/cit/output"))) {
            fs.delete(new Path("/cit/output"), true);
        }

        job.waitForCompletion(true);
    }


    @Test
    public void testStart() throws InterruptedException, IOException, ClassNotFoundException {
        String vectorFilePath = "/cit/input/doc";
        String outputDir = "/cit/output";
        int k = 20;
        CentersInitializer.start(vectorFilePath, outputDir, k);
    }

}
