package lab3.achieve;

import common.AbstractLoggedMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 05.04.2015
 */
public class SimpleCounterMapper extends AbstractLoggedMapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text("auto_visits");

    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        infoMsg("Value: " + value.toString());

        String[] split = value.toString().split("\t");
        infoMsg("Split size: " + split.length);

        if (split.length == 2) {
            int v = Integer.valueOf(split[1]);

            if (v == 1) outputCollector.collect(word, new IntWritable(v));
        }
    }
}
