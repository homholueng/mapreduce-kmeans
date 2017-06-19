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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * 用于初始化簇心，需要在 Configuration 设置 K 和 docIDFilePath,
 * K: 指定有几个簇,
 * docIDFilePath: 用于存放所有网页ID的DistributedCache文件路径,
 * 启动入口在 start()
 * Created by yuan on 17-6-15.
 */
public class CentersInitializer {

    public static final String K = "K";

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        private static Set<String> docIDSet = new HashSet<String>();
        private static Map<String, String> centers = new HashMap<String, String>();

        private Text centerID = new Text();


        /**
         * 初始化中心
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get(K));
            String docIDFilePath = conf.get(Constants.IDS_FILE_PATH_NAME);
            fetchFile2Set(docIDFilePath, context);
            initialCenters(k);
            super.setup(context);
        }

        /**
         * 初始化k个中心点
         * @param k
         */
        private void initialCenters(int k) {
            ArrayList<String> list = new ArrayList<String>(docIDSet);
            Collections.shuffle(list);
            for (int i = 0; i < k; i++) {
                centers.put(list.get(i), String.valueOf(i));
            }
        }

        /**
         * 把DistributedCache文件的网页ID取出到集合中
         * @param docIDFilePath
         * @param context
         * @throws IOException
         */
        private void fetchFile2Set(String docIDFilePath, Context context) throws IOException {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            Path path = new Path(docIDFilePath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
            String docID;
            while ((docID = reader.readLine()) != null) {
                docIDSet.add(docID);
            }
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
//            System.out.println("key: " + key.toString() + ", value: " + value.toString());
            if (centers.containsKey(key.toString())) {
                centerID.set(centers.get(key.toString()));
                context.write(centerID, value);
            }
        }
    }


    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value: values) {
                context.write(key, value);
            }
        }
    }


    /**
     * CentersInitializer 启动入口
     * @param vectorFilePath 存放网页向量的文件路径或者目录（网页ID->v1&v2&v3...）
     * @param outputDir 输出目录
     * @param k 要聚类的个数
     * @throws InterruptedException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void start(String vectorFilePath, String outputDir, int k) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();
        KmeansDriver.initProcessCache(conf);

        conf.set(Constants.IDS_FILE_PATH_NAME, KmeansDriver.IDS_CACHE_PATH);
        conf.set(CentersInitializer.K, String.valueOf(k));
        Job job = Job.getInstance(conf);
        job.setJarByClass(CentersInitializer.class);
        job.setMapperClass(CentersInitializer.MyMapper.class);
        job.setReducerClass(CentersInitializer.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        KeyValueTextInputFormat.addInputPath(job, new Path(vectorFilePath));
        SequenceFileInputFormat.addInputPath(job, new Path(vectorFilePath));
        job.setInputFormatClass(SequenceFileInputFormat.class);

//        TextOutputFormat.setOutputPath(job, new Path("/cit/output"));
        SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        job.waitForCompletion(true);
    }
    public static void startWithConf(String vectorFilePath, String outputDir, int k, Configuration conf) throws InterruptedException, IOException, ClassNotFoundException {
        conf.get(Constants.IDS_FILE_PATH_NAME, KmeansDriver.IDS_CACHE_PATH);
        conf.set(CentersInitializer.K, String.valueOf(k));
        Job job = Job.getInstance(conf);
        job.setJarByClass(CentersInitializer.class);
        job.setMapperClass(CentersInitializer.MyMapper.class);
        job.setReducerClass(CentersInitializer.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        KeyValueTextInputFormat.addInputPath(job, new Path(vectorFilePath));
        SequenceFileInputFormat.addInputPath(job, new Path(vectorFilePath));
        job.setInputFormatClass(SequenceFileInputFormat.class);

//        TextOutputFormat.setOutputPath(job, new Path("/cit/output"));
        SequenceFileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outputDir))) {
            fs.delete(new Path(outputDir), true);
        }

        job.waitForCompletion(true);
    }

}
