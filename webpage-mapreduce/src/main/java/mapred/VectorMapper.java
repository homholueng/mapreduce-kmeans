package mapred;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Leaf on 2017/6/15.
 */
public class VectorMapper extends Mapper<Text, DoubleWritable, Text, Text> {
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
        String[] wordAndId = key.toString().split("|");
        context.write(new Text(wordAndId[1]), new Text(wordAndId[0] + "|" + value.toString()));
    }
}
