package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static mapred.config.Constants.SEPERATOR;

/**
 * Created by HL on 14/06/2017.
 */
public class WordPercentCounter {

    public static class MapClass extends Mapper<Text, IntWritable, Text, Text> {

        // input: 单词|网页编号->单词在网页编号出现次数
        // output: 网页编号->单词|单词在网页编号出现次数
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString();
            String[] fields = keyStr.split(SEPERATOR);
            String word = fields[0];
            String id = fields[1];
//            System.out.println(id + ": " + word);
            StringBuilder valueBuilder = new StringBuilder();
            context.write(new Text(id), new Text(valueBuilder.append(word).append(SEPERATOR).append(value.get()).toString()));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {

        // input: 网页编号->单词|单词在网页编号出现次数
        // output:单词|网页编号->单词在网页编号出现次数/网页的总单词数
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> occursTimes = new HashMap<String, Double>(); // <word|id, 123>
            double wordsCount = 0;
            String id = key.toString();
            for (Text val : values) {
                String[] fields = val.toString().split(SEPERATOR);
                String word = fields[0];
                String times = fields[1];
                occursTimes.put((new StringBuilder()).append(word).append(SEPERATOR).append(id).toString(),
                        new Double(times));
                wordsCount += 1;
            }
            Set<String> keySet = occursTimes.keySet();
            for (String mapKey : keySet) {
                double percent = occursTimes.get(mapKey) / wordsCount;
//                System.out.println(percent);
                context.write(new Text(mapKey), new DoubleWritable(percent));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordPercentCounter.class);
        job.setMapperClass(WordPercentCounter.MapClass.class);
        job.setReducerClass(WordPercentCounter.Reduce.class);

        Path in = new Path("mapred1_output_test/part-r-00000");
        Path out = new Path("mapred2_output_test");
        FileSystem.get(configuration).delete(out, true);
        SequenceFileInputFormat.addInputPath(job, in);
        SequenceFileOutputFormat.setOutputPath(job, out);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.waitForCompletion(true);
    }
}
