import mapred.CentersInitializer;
import mapred.KmeansDriver;
import mapred.config.Constants;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by yuan on 17-6-15.
 */
public class CentersInitializerTest {


    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private Text docID;
        private Text content = new Text("Hello World");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            docID = value;
            context.write(docID, content);
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
    public static void creatTestData() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        KmeansDriver.initProcessCache(conf);

        Job job = Job.getInstance(conf);
        job.setJarByClass(CentersInitializerTest.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path("/cit/input/01"));
        TextOutputFormat.setOutputPath(job, new Path("/cit/output"));

        job.waitForCompletion(true);
    }


    public static void test() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        KmeansDriver.initProcessCache(conf);

        conf.set(CentersInitializer.K, "2");
        Job job = Job.getInstance(conf);
        job.setJarByClass(CentersInitializer.class);
        job.setMapperClass(CentersInitializer.MyMapper.class);
        job.setReducerClass(CentersInitializer.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path("/cit/input/doc"));
//        FileOutputFormat.setOutputPath(job, new Path("/cit/output"));
//        TextInputFormat.addInputPath(job, new Path("/cit/input/doc"));
//        TextOutputFormat.setOutputPath(job, new Path("/cit/output"));
        KeyValueTextInputFormat.addInputPath(job, new Path("/cit/input/doc"));
        TextOutputFormat.setOutputPath(job, new Path("/cit/output"));
        job.setInputFormatClass(KeyValueTextInputFormat.class);


        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
//        creatTestData();
        test();
    }
}
