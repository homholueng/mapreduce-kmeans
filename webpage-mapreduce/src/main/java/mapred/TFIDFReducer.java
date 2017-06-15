package mapred;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Leaf on 2017/6/15.
 */
public class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
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
        // 在任务中将任务名设置为总文档数,这是一个常量
        int totalDocuments = Integer.parseInt(context.getJobName());
        Map<String, String> map = new HashMap<String, String>();
        // 该单词在多少个文档中出现
        int totalDocumentsWhichAppearTerm = 0;
        for (Text value : values) {
            totalDocumentsWhichAppearTerm += 1;
            String[] idAndTF = value.toString().split("|");
            map.put(idAndTF[0], idAndTF[1]);
        }
        for (String id : map.keySet()) {
            Double TF = Double.parseDouble(map.get(id));
            DoubleWritable TFIDF =
                    new DoubleWritable(TF * Math.log((double)totalDocuments / totalDocumentsWhichAppearTerm));
            context.write(new Text(key.toString() + "|" + id), TFIDF);
        }
    }
}
