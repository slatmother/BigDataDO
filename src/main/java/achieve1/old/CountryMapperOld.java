package achieve1.old;

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
public class CountryMapperOld extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger LOG = Logger.getLogger(CountryMapperOld.class);

    private Text url = new Text();

    @Override
    public void map(LongWritable text, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        LOG.info("Mapper: Value: " + value.toString());

        String[] splittedIn = value.toString().split(" ");
        LOG.info("Mapper: Split size: " + splittedIn.length);

        if (splittedIn.length == 3) {
            try {
               new Long(splittedIn[0]);
            } catch (NumberFormatException e) {
                LOG.error("Is not a number: " + splittedIn[0]);
                return;
            }

            url.set(splittedIn[2].trim());
            outputCollector.collect(url, new IntWritable(1));
        } else {
            LOG.info("Split array has length not 3!!!");
        }

    }
}
