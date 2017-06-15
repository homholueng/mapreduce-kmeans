package util;

import org.lionsoul.jcseg.tokenizer.core.*;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by HL on 15/06/2017.
 */
public class SegmentorFactory {
    public static Segmentor newInstance() {
        ISegment seg = null;
        JcsegTaskConfig config = new JcsegTaskConfig(true);
        config.setClearStopwords(true);
        config.setAppendCJKSyn(false);

        ADictionary dic = DictionaryFactory.createSingletonDictionary(config);
        try {
            seg = SegmentFactory.createJcseg(
                    JcsegTaskConfig.COMPLEX_MODE,
                    new Object[]{config, dic}
            );
        } catch (JcsegException e) {
            e.printStackTrace();
        }
        return new Segmentor(seg);
    }

    public static class Segmentor {
        private ISegment seg;

        private Segmentor(ISegment seg) {
            this.seg = seg;
        }

        public void reset(String content) throws IOException {
            seg.reset(new StringReader(content));
        }

        public IWord next() throws IOException {
            IWord word = null;
            while ((word = seg.next()) != null) {
                int t = word.getType();
                if (t == 1 || t == 2 || t == 5) {
                    break;
                }
            }
            return word;
        }
    }
}
