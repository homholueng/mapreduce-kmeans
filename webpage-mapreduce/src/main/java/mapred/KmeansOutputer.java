package mapred;

import mapred.config.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 使用经过 Kmeans 训练而产生的中心点向量集合来对网页进行聚类,
 * 启动入口在 start()
 * Created by yuan on 17-6-16.
 */
public class KmeansOutputer {

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        private static Map<String, double[]> centers = new HashMap<String, double[]>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Kmeans.readCentersFile(context, centers);
        }

        /**
         * 输入： 网页编号->v1&v2&v3...
         * 输出： 中心点编号->网页编号
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            double[] vector = Kmeans.convert(value.toString());
            String nearestCenterID = Kmeans.findNearestCenter(vector, centers);
            value.set(key.toString());
            key.set(nearestCenterID);
            context.write(key, value);
        }
    }


    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * 输入： 中心点编号->网页编号1,网页编号2
         * 输出： 中心点编号->网页编号
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    /**
     * KmeansOutputer 启动入口
     * @param vectorFilePath 存放网页向量的文件路径或者目录（网页ID->v1&v2&v3...）
     * @param centersFilePath 存放中心点向量的文件路径（中心点ID->v1&v2&v3...）
     * @param outputDir 输出目录
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void start(String vectorFilePath, String centersFilePath, String outputDir) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        conf.set(Constants.VECTOR_SEPERATOR, "&");
        conf.set(Kmeans.CENTERS_PATH, centersFilePath);
        Job job = Job.getInstance(conf);
        job.setJarByClass(KmeansOutputer.class);
        job.setMapperClass(KmeansOutputer.MyMapper.class);
        job.setReducerClass(KmeansOutputer.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


//        KeyValueTextInputFormat.addInputPath(job, new Path(vectorFilePath));
//        job.setInputFormatClass(KeyValueTextInputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(vectorFilePath));
        job.setInputFormatClass(SequenceFileInputFormat.class);

//        SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        TextOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setOutputFormatClass(TextOutputFormat.class);


        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        job.waitForCompletion(true);

    }
    public static void startWithConf(String vectorFilePath, String centersFilePath, String outputDir, Configuration conf) throws InterruptedException, IOException, ClassNotFoundException {
        FileSystem fs = FileSystem.get(conf);

        conf.set(Constants.VECTOR_SEPERATOR, "&");
        conf.set(Kmeans.CENTERS_PATH, centersFilePath);
        Job job = Job.getInstance(conf);
        job.setJarByClass(KmeansOutputer.class);
        job.setMapperClass(KmeansOutputer.MyMapper.class);
        job.setReducerClass(KmeansOutputer.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


//        KeyValueTextInputFormat.addInputPath(job, new Path(vectorFilePath));
//        job.setInputFormatClass(KeyValueTextInputFormat.class);

        SequenceFileInputFormat.addInputPath(job, new Path(vectorFilePath));
        job.setInputFormatClass(SequenceFileInputFormat.class);

//        SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        TextOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setOutputFormatClass(TextOutputFormat.class);


        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        job.waitForCompletion(true);
    }
}
