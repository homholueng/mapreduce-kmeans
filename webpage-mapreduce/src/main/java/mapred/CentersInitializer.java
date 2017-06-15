package mapred;

import mapred.config.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.*;

/**
 * 用于初始化簇心，需要在 Configuration 设置 K 和 docIDFilePath
 * K: 指定有几个簇
 * docIDFilePath: 用于存放所有网页ID的DistributedCache文件路径
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
            if (centers.containsKey(key)) {
                centerID.set(centers.get(key));
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

    public static void main(String[] args) {

    }
}
