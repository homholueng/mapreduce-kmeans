package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.Map;

/**
 * 用于 Kmeans 迭代产出中心点集合
 * Created by yuan on 17-6-16.
 */
public class Kmeans {

    public static final String CENTERS_PATH = "CENTERS_PATH";

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        private static Map<String, double[]> centers;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path centersPath = new Path(conf.get(CENTERS_PATH));
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, centersPath, conf);

        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }
}
