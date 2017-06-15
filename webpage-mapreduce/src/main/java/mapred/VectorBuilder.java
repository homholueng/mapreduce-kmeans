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

/**
 * Created by Leaf on 2017/6/15.
 */
public class VectorBuilder {
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
            context.write(new Text(wordAndId[1]), new Text(wordAndId[0] + SEPERATOR + value.toString()));
        }
    }

    public static class VectorBuilderReducer extends Reducer<Text, Text, Text, Text> {
        private static Map<String, Integer> WORD_DICT = new HashMap<String, Integer>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 从文件构建出单词表
            Configuration conf = context.getConfiguration();
            Path dictPath = new Path(conf.get("DICTPATH"));
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
         * @param context 输出: TfIdf1&TfIdf2&TfIdf3...
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder vectorString = new StringBuilder();
            String[] vector = new String[WORD_DICT.size()];
            for (String v : vector) {
                v = "0";
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
}
