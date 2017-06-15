import junit.framework.TestCase;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Created by Leaf on 2017/6/15.
 */
public class TFIDFMapperTest extends TestCase {
    private Mapper<Text, DoubleWritable, Text, Text> mapper;
}
