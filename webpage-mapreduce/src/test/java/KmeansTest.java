import mapred.CentersInitializer;
import mapred.Kmeans;
import mapred.KmeansDriver;
import mapred.config.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by yuan on 17-6-16.
 */
public class KmeansTest {

    /**
     * 测试运行一个 Kmeans
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Test
    public void testIterationOnce() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        KmeansDriver.initProcessCache(conf);

        conf.set(Constants.VECTOR_SEPERATOR, "&");
        conf.set(Kmeans.CENTERS_PATH, "/cit/output/part-r-00000");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Kmeans.class);
        job.setMapperClass(Kmeans.MyMapper.class);
        job.setReducerClass(Kmeans.MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path("/km/input/doc"));
//        FileOutputFormat.setOutputPath(job, new Path("/km/output"));
//        TextInputFormat.addInputPath(job, new Path("/km/input/doc"));
//        TextOutputFormat.setOutputPath(job, new Path("/km/output"));
        KeyValueTextInputFormat.addInputPath(job, new Path("/km/input"));
        TextOutputFormat.setOutputPath(job, new Path("/km/output"));
        job.setInputFormatClass(KeyValueTextInputFormat.class);

//        SequenceFileOutputFormat.setOutputPath(job, new Path("/km/output"));
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        job.waitForCompletion(true);
    }


    @Test
    public void testStart() throws InterruptedException, IOException, ClassNotFoundException {
        String vectorFilePath = "/cit/input/doc";
        String centersFilePath = "/cit/output/part-r-00000";
        String outputDir = "/km/output";
        int totalIter = 3;
        Kmeans.start(vectorFilePath, centersFilePath, outputDir, totalIter);
    }




}
