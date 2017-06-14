package mapred;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.lionsoul.jcseg.tokenizer.core.*;

import java.io.IOException;
import java.io.StringReader;

import static hbase.config.TableConfig.CONTENT_FAMILY;
import static hbase.config.TableConfig.CONTENT_QUALIFIER;
import static mapred.conf.Constants.SEPERATOR;

/**
 * Created by HL on 14/06/2017.
 */
public class Mapred1 {

    public static class MapClass extends TableMapper<Text, LongWritable> {

        ISegment seg;

        public MapClass() {
            JcsegTaskConfig config = new JcsegTaskConfig(true);
            config.setClearStopwords(true);
            config.setAppendCJKSyn(false);

            ADictionary dic = DictionaryFactory.createSingletonDictionary(config);
            try {
                this.seg = SegmentFactory.createJcseg(
                        JcsegTaskConfig.COMPLEX_MODE,
                        new Object[]{config, dic}
                );
            } catch (JcsegException e) {
                e.printStackTrace();
            }
        }

        // input: 网页编号->网页内容
        // output: 单词|网页编号->1
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String id = key.toString();
            byte[] bytes = value.getValue(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER));
            String content = new String(bytes);

            seg.reset(new StringReader(content));
            IWord word = null;
            while ((word = seg.next()) != null) {
                int t = word.getType();
                if (t == 1 || t == 2 || t == 5) {
                    String val = word.getValue();
                    StringBuilder keyBuilder = new StringBuilder(val.length() + SEPERATOR.length() + id.length());
                    context.write(new Text(keyBuilder.append(val).append(SEPERATOR).append(id).toString()),
                            new LongWritable(1));
                }
            }
        }
    }

    // input: 单词|网页编号->1
    // output: 单词|网页编号->单词在网页编号出现次数字
    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (LongWritable val : values) {
                count += 1;
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) {

    }

}
