package achieve2.old;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 16.03.2015
 */
public class URLMapperOld extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger LOG = Logger.getRootLogger();

    private Text url = new Text();

    @Override
    public void map(LongWritable text, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String[] splitIn = value.toString().split("\t");
        LOG.info("<" + URLMapperOld.class.toString() + "> Split size: " + splitIn.length);

        if (splitIn.length == 3) {
            try {
                new Long(splitIn[0]);
            } catch (NumberFormatException e) {
                LOG.warn("<" + URLMapperOld.class.toString() + "> Not a number! [" + splitIn[0] + "]");
                return;
            }

            url.set(splitIn[2].trim());
            outputCollector.collect(url, new IntWritable(1));
        } else {
            LOG.warn("<" + URLMapperOld.class.toString() + "> Incorrect split: [" + value.toString() + "]:[" + splitIn.length + "]");
        }
    }
}
