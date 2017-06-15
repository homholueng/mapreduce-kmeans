package mapred;

import mapred.config.Constants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.lionsoul.jcseg.tokenizer.core.IWord;
import util.SegmentorFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static hbase.config.TableConfig.CONTENT_FAMILY;
import static hbase.config.TableConfig.CONTENT_QUALIFIER;
import static mapred.config.Constants.SEPERATOR;
import static util.SegmentorFactory.Segmentor;

/**
 * Created by HL on 14/06/2017.
 */
public class Mapred1 {

    public static class MapClass extends TableMapper<Text, IntWritable> {

        Segmentor seg;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.seg = SegmentorFactory.newInstance();
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            Path path = new Path(context.getConfiguration().get(Constants.IDS_FILE_PATH_NAME));
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
            String id = "";
            while ((id = reader.readLine()) != null) {
                System.out.println(id);
            }
        }

        // input: 网页编号->网页内容
        // output: 单词|网页编号->1
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String id = key.toString();
            byte[] bytes = value.getValue(Bytes.toBytes(CONTENT_FAMILY), Bytes.toBytes(CONTENT_QUALIFIER));
            String content = new String(bytes);

            seg.reset(content);
            IWord word = null;
            while ((word = seg.next()) != null) {
                String val = word.getValue();
                StringBuilder keyBuilder = new StringBuilder(val.length() + SEPERATOR.length() + id.length());
                Text keyout = new Text(keyBuilder.append(val).append(SEPERATOR).append(id).toString());
//                System.out.println(keyout + ": " + val);
                context.write(keyout,
                        new IntWritable(1));
            }
        }
    }

    // input: 单词|网页编号->1
    // output: 单词|网页编号->单词在网页编号出现次数字
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += 1;
            }
            context.write(key, new IntWritable(count));
        }
    }
}
