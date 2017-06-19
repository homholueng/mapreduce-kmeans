package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static mapred.config.Constants.SEPERATOR;
import static mapred.config.Constants.PAGE_COUNT_NAME;
import static mapred.config.Constants.WORDS_FILE_PATH_NAME;

/**
 * Created by Leaf on 2017/6/15.
 */
public class TfIdf extends Configured implements Tool {
    public static class TFIDFMapper extends Mapper<Text, DoubleWritable, Text, Text> {
        /**
         *
         * @param key 单词|网页编号
         * @param value 单词在网页编号中出现的次数/网页的总单词数
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            // 测试用
//            String[] keyValue = value.toString().split("\t");
//            String[] wordAndId = keyValue[0].split(SEPERATOR);
//            System.out.println(wordAndId[0]);
//            context.write(new Text(wordAndId[0]), new Text(wordAndId[1] + SEPERATOR + keyValue[1]));
            String[] wordAndId = key.toString().split(SEPERATOR);
            StringBuilder sb = new StringBuilder();
            sb.append(wordAndId[1]).append(SEPERATOR).append(value.get());
            context.write(new Text(wordAndId[0]), new Text(sb.toString()));
        }
    }

    public static class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private static Map<String, Integer> WORD_DICT = new HashMap<String, Integer>();
        private static int WORDINDEX = 0;
        private long totalDocuments;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            totalDocuments = conf.getInt(PAGE_COUNT_NAME, 100000);
            super.setup(context);
        }

        /**
         *
         * @param key 单词
         * @param values 网页编号|单词在网页中出现的次数/网页的总单词数
         * @param context 输出格式为 单词|网页编号 => TFIDF值
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 建立一个单词表
            if (!WORD_DICT.containsKey(key.toString())) {
                WORD_DICT.put(key.toString(), WORDINDEX);
                WORDINDEX++;
            }
            // 在任务中将任务名设置为总文档数,这是一个常量
            Map<String, String> map = new HashMap<String, String>();
            // 该单词在多少个文档中出现
            int totalDocumentsWhichAppearTerm = 0;
            for (Text value : values) {
                totalDocumentsWhichAppearTerm += 1;
                String[] idAndTF = value.toString().split(SEPERATOR);
                map.put(idAndTF[0], idAndTF[1]);
            }
            for (String id : map.keySet()) {
                Double TF = Double.parseDouble(map.get(id));
                DoubleWritable TFIDF =
                        new DoubleWritable(TF * Math.log((double)totalDocuments / totalDocumentsWhichAppearTerm));
                context.write(new Text(key.toString() + SEPERATOR + id), TFIDF);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 将构建的单词表写的文件里面去
            Configuration conf = context.getConfiguration();
            Path path = new Path(conf.get(WORDS_FILE_PATH_NAME));
            FileSystem fs = FileSystem.get(conf);
            fs.delete(path, true);
            SequenceFile.Writer.Option outPath = SequenceFile.Writer.file(path);
            SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(Text.class);
            SequenceFile.Writer.Option optValue = SequenceFile.Writer.valueClass(IntWritable.class);
            final SequenceFile.Writer out = SequenceFile.createWriter(conf, outPath, optKey, optValue);
            for (String word : WORD_DICT.keySet()) {
                out.append(new Text(word), new IntWritable(WORD_DICT.get(word)));
            }
            out.close();
            super.cleanup(context);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: mapred.TfIdf <input path> <output path>");
        }

        Configuration conf = getConf();
        conf.setInt(PAGE_COUNT_NAME, 10);
        conf.set(WORDS_FILE_PATH_NAME, "/Users/Leaf/Desktop/dict.txt");
        FileSystem fs = FileSystem.get(conf);

        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf);


        FileInputFormat.addInputPath(job, inPath);


        job.setJarByClass(TfIdf.class);
        job.setJobName("TFIDF");
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outPath);
        job.setMapperClass(TfIdf.TFIDFMapper.class);
        job.setReducerClass(TfIdf.TFIDFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TfIdf(), args);
        System.exit(res);
    }
}
