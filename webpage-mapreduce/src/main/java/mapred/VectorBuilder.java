package mapred;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static mapred.config.Constants.SEPERATOR;
import static mapred.config.Constants.WORDS_FILE_PATH_NAME;

/**
 * Created by Leaf on 2017/6/15.
 */
public class VectorBuilder extends Configured implements Tool {
    public static class VectorBuilderMapper extends Mapper<Text, DoubleWritable, Text, Text> {
        /**
         *
         * @param key 单词|网页编号
         * @param value TFIDF 值
         * @param context 输出: 网页编号 => 单词|TFIDF值
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            String[] wordAndId = key.toString().split(SEPERATOR);
            System.out.println(wordAndId[0]);
            context.write(new Text(wordAndId[1]), new Text(wordAndId[0] + SEPERATOR + value.toString()));
        }
    }

    public static class VectorBuilderReducer extends Reducer<Text, Text, Text, Text> {
        private static Map<String, Integer> WORD_DICT = new HashMap<String, Integer>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 从文件构建出单词表
            Configuration conf = context.getConfiguration();
            Path dictPath = new Path(conf.get(WORDS_FILE_PATH_NAME));
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Reader.Option inPath = SequenceFile.Reader.file(dictPath);
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, inPath);
            Text key = new Text();
            IntWritable value = new IntWritable();
            while (reader.next(key, value)) {
                WORD_DICT.put(key.toString(), value.get());
            }
            reader.close();
        }

        /**
         *
         * @param key 网页编号
         * @param values 单词|TFIDF 值
         * @param context 输出: 网页编号 => TfIdf1&TfIdf2&TfIdf3...
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder vectorString = new StringBuilder();
            String[] vector = new String[WORD_DICT.size()];
            for (int i = 0; i < vector.length; i++) {
                vector[i] = "0";
            }
            for (Text value : values) {
                String[] wordAndTfIdf = value.toString().split(SEPERATOR);
                if (WORD_DICT.containsKey(wordAndTfIdf[0])) {
                    vector[WORD_DICT.get(wordAndTfIdf[0])] = wordAndTfIdf[1];
                }
            }
            for (int i = 0; i < vector.length; i++) {
                vectorString.append(vector[i]);
                vectorString.append("&");
            }
            vectorString.deleteCharAt(vectorString.length() - 1);
            context.write(key, new Text(vectorString.toString()));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: <input path> <output path>");
        }
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        conf.set(WORDS_FILE_PATH_NAME, "/Users/Leaf/Desktop/dict.txt");
        Job job = Job.getInstance(conf);
        job.setJobName("Vector Builder");

        job.setJarByClass(VectorBuilder.class);
        job.setMapperClass(VectorBuilder.VectorBuilderMapper.class);
        job.setReducerClass(VectorBuilder.VectorBuilderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VectorBuilder(), args);
        System.exit(res);
    }
}
