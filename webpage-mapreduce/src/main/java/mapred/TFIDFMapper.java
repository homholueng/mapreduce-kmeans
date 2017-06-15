package mapred;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Leaf on 2017/6/15.
 */

/* NOTE: 我感觉从上层传来的 单词在网页编号中出现的次数/网页的总单词数 可以用字符串表示（Text)
         即形如 "86/666" 这种形式的字符串
 */
public class TFIDFMapper extends Mapper<Text, DoubleWritable, Text, Text> {
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
        String[] wordAndId = key.toString().split("|");
        context.write(new Text(wordAndId[0]), new Text(wordAndId[1] + "|" + value.toString()));
    }
}
