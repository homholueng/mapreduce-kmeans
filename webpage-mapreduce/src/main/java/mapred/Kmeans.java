package mapred;

import mapred.config.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 用于 Kmeans 迭代产出中心点集合
 * Created by yuan on 17-6-16.
 */
public class Kmeans {

    /**
     * 把字符串形式(v1&v2&v3&v4...)的向量转化为double[]的形式
     * @param s
     * @return
     */
    private static double[] convert(String s) {
        String[] strs = s.split(Constants.VECTOR_SEPERATOR);
        int len = strs.length;
        double[] vector = new double[len];
        for (int i = 0; i < len; i++) {
            vector[i] = Double.parseDouble(strs[i]);
        }
        return vector;
    }


    /**
     * 把 double[] 形式的向量转化为字符串形式（v1&v2&v3...）
     * @param v
     * @return
     */
    public static String convert(double[] v) {
        StringBuilder sb = new StringBuilder();
        sb.append(v[0]);
        for (int i = 1; i < v.length; i++) {
            sb.append("&" + v[i]);
        }
        return sb.toString();
    }

    public static final String CENTERS_PATH = "CENTERS_PATH";

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        private static Map<String, double[]> centers = new HashMap<String, double[]>();


        /**
         * 读取初始或者上次迭代产生的中心点集合的文件
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path centersPath = new Path(conf.get(CENTERS_PATH));
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, centersPath, conf);
            Text centerID = new Text();
            Text value = new Text();
            while (reader.next(centerID, value)) {
                double[] vector = convert(value.toString());
                centers.put(centerID.toString(), vector);
            }

        }


        /**
         * 输入： 网页ID->v1&v2&v3...
         * 输出： 中心点ID->v1&v2&3...
         * 为每一个网页找到最近的中心，然后把Key改为该中心ID再输出到Reduce
         * @param key 网页ID
         * @param value 词向量的字符串表示（1&2&3&4...）
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            double[] vector = convert(value.toString());
            String nearestCenterID = findNearestCenter(vector);
            key.set(nearestCenterID);
            context.write(key, value);
        }

        /**
         * 找到与当前 docVector 距离最近的中点编号
         * @param docVector
         * @return
         */
        private String findNearestCenter(double[] docVector) {
            double minDistance = Double.MAX_VALUE;
            double curDistance;
            double[] centerVector;
            String nearestCenterID = null;
            for (Map.Entry<String, double[]> center : centers.entrySet()) {
                centerVector = center.getValue();
                curDistance = calDistance(centerVector, docVector);
                if (curDistance < minDistance) {
                    nearestCenterID = center.getKey();
                    minDistance = curDistance;
                }
            }
            return nearestCenterID;
        }

        /**
         * 计算欧式距离
         * @param centerVector
         * @param docVector
         * @return
         */
        private double calDistance(double[] centerVector, double[] docVector) {
            double sum = 0;
            for (int i = 0; i < centerVector.length && i < docVector.length; i++) {
                sum += Math.pow(centerVector[i] - docVector[i], 2);
            }
            sum = Math.sqrt(sum);
            return sum;
        }
    }


    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        private Text value = new Text();


        private static double[] initialNewVector(int len) {
            double[] v = new double[len];
            for (int i = 0; i < len; i++) {
                v[i] = 0;
            }
            return v;
        }

        /**
         * 对向量取平均值
         * @param v
         * @param count
         */
        private static void average(double[] v, int count) {
            for (int i = 0; i < v.length; i++) {
                v[i] /= count;
            }
        }

        /**
         * 把 v2 加到 v1 上，所以 v2 不会发生改变， v1 会发生改变
         * @param v1
         * @param v2
         */
        private static void add(double[] v1, double[] v2) {
            for (int i = 0; i < v1.length && i < v2.length; i++) {
                v1[i] += v2[i];
            }
        }

        /**
         * 输入： 中心点编号->v1&v2&v3...
         * 输出： 中心点编号->v2&v2&v3...
         * 计算新的中心点向量
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[] newVector = null;
            int count = 0;
            for (Text value : values) {
                double[] vector = convert(value.toString());
                if (newVector == null) {
                    newVector = initialNewVector(vector.length);
                }
                add(newVector, vector);
                count++;
            }
            average(newVector, count);
            value.set(convert(newVector));
            context.write(key, value);
        }

    }
}
