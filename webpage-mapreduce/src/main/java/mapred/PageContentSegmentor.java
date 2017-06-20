package mapred;

import hbase.config.TableConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.lionsoul.jcseg.tokenizer.core.IWord;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import util.SegmentorFactory;

import java.io.IOException;

import static hbase.config.TableConfig.*;
import static mapred.config.Constants.SEPERATOR;
import static util.SegmentorFactory.Segmentor;

/**
 * Created by HL on 14/06/2017.
 */
public class PageContentSegmentor {

    public static class MapClass extends TableMapper<Text, IntWritable> {

//        Segmentor seg;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            this.seg = SegmentorFactory.newInstance();
        }

        // input: 网页编号->网页内容
        // output: 单词|网页编号->1
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String id = new String(key.get());
            byte[] bytes = value.getValue(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER));
            String content = new String(bytes);

            Segmentor seg = SegmentorFactory.newInstance();
            seg.reset(content);
            IWord word = null;
            while ((word = seg.next()) != null) {
                String val = word.getValue();
                StringBuilder keyBuilder = new StringBuilder(val.length() + SEPERATOR.length() + id.length());
                Text keyout = new Text(keyBuilder.append(val).append(SEPERATOR).append(id).toString());
                context.write(keyout,
                        new IntWritable(1));
            }
        }
    }

    // input: 单词|网页编号->1
    // output: 单词|网页编号->单词在网页编号出现次数字
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += 1;
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("mapred.textoutputformat.separator", "\t");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageContentSegmentor.class);
        job.setMapperClass(PageContentSegmentor.MapClass.class);
        job.setReducerClass(PageContentSegmentor.Reduce.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(CONTENT_FAMILY));
        // good for map reduce job
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        Path out = new Path("mapred1_output_test");
        FileSystem.get(configuration).delete(out, true);

        TableMapReduceUtil.initTableMapperJob(
                TableConfig.NEWS_TABLE_NAME,
                scan,
                PageContentSegmentor.MapClass.class,
                Text.class,
                IntWritable.class,
                job
        );

        SequenceFileOutputFormat.setOutputPath(job, out);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.waitForCompletion(true);
    }
}
