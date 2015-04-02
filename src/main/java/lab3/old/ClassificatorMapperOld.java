package lab3.old;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Arrays;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 16.03.2015
 */
public class ClassificatorMapperOld extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
    private static final Logger LOG = Logger.getRootLogger();

    private Text url = new Text();
    private LongWritable uid = new LongWritable();

    @Override
    public void map(LongWritable text, Text value, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
        infoMsg("Value: " + value.toString());

        String[] split = value.toString().split("\t");
        infoMsg("Split size: " + split.length);

        if (split.length == 3) {
            infoMsg("Split: " + Arrays.asList(split));

            String domain = null;

            try {
                String urlRaw = URLDecoder.decode(split[2], "UTF-8");

                URI urlP = new URI(urlRaw);
                domain = urlP.getHost();

                if (domain != null) {
                    domain = domain.startsWith("www.") ? domain.substring("www.".length()) : domain;
                    infoMsg("Result domain: " + domain);
                } else {
                    infoMsg("Domain from URL [" + urlRaw + "] has not been resolved!");
                    return;
                }
            } catch (Exception e) {
                errorMsg(e);
                return;
            }

            url.set(domain);

            try {
                uid.set(new Long(split[0]));
            } catch (NumberFormatException e) {
                errorMsg("Not a long value: [" + split[0] + "]");
            }

            outputCollector.collect(uid, url);
        } else {
            infoMsg("Split array has length not 3!!!");
        }
    }

    private void infoMsg(String msg) {
        LOG.info("<" + this.getClass().toString() + "> " + msg);
    }

    private void errorMsg(String msg) {
        LOG.error("<" + this.getClass().toString() + "> " + msg);
    }

    private void errorMsg(Throwable e) {
        LOG.error("<" + this.getClass().toString() + "> " + e);
    }
}
