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
    private static final Logger LOG = Logger.getRootLogger();

    private Text url = new Text();

    @Override
    public void map(LongWritable text, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        infoMsg("Mapper: Value: " + value.toString());

        String[] splittedIn = value.toString().split("\t");
        infoMsg("Mapper: Split size: " + splittedIn.length);

        if (splittedIn.length == 3) {
            try {
               new Long(splittedIn[0]);
            } catch (NumberFormatException e) {
                errorMsg("Is not a number: " + splittedIn[0]);
                return;
            }

            url.set(splittedIn[2].trim());
            outputCollector.collect(url, new IntWritable(1));
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
}
