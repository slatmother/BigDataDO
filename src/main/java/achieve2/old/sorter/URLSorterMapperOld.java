package achieve2.old.sorter;

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
public class URLSorterMapperOld extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger LOG = Logger.getRootLogger();

    private Text url = new Text();

    @Override
    public void map(LongWritable text, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String[] splitIn = value.toString().split("\t");
        LOG.info("<" + URLSorterMapperOld.class.toString() + "> Split size: " + splitIn.length);
        url.set(splitIn[0].trim());
//        outputCollector.collect(new IntWritable(new Integer(splitIn[1])), url);
    }
}
