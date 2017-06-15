package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static mapred.config.Constants.SEPERATOR;
import static mapred.config.Constants.PAGE_COUNT_NAME;

/**
 * Created by Leaf on 2017/6/15.
 */
public class TfIdf {
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
            String[] wordAndId = key.toString().split(SEPERATOR);
            context.write(new Text(wordAndId[0]), new Text(wordAndId[1] + SEPERATOR + value.toString()));
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
            Path path = new Path(conf.get("DICPATH"));
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
}
