package mapred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Leaf on 2017/6/15.
 */
public class VectorReducer extends Reducer<Text, Text, Text, Text> {
    /**
     *
     * @param key 网页编号
     * @param values 单词|TFIDF 值
     * @param context 输出: 单词编号1|TFIDF值1&单词编号2|TFIDF值
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder vectorString = new StringBuilder();
        for (Text value : values) {
            vectorString.append(value.toString());
            vectorString.append("&");
        }
        vectorString.deleteCharAt(vectorString.length() - 1);
        context.write(key, new Text(vectorString.toString()));
    }
}
