import mapred.KmeansOutputer;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by yuan on 17-6-16.
 */
public class KmeansOutputerTest {

    @Test
    public void testStart() throws InterruptedException, IOException, ClassNotFoundException {
        String vectorFilePath = "/cit/input/doc";
        String centersFilePath = "/km/output/part-r-00000";
        String outputDir = "/final/output";
        KmeansOutputer.start(vectorFilePath, centersFilePath, outputDir);
    }
}
