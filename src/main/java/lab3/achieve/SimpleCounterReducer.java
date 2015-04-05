package lab3.achieve;

import common.AbstractLoggedReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 05.04.2015
 */
public class SimpleCounterReducer extends AbstractLoggedReducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int totalAuto = 0;

        while (iterator.hasNext()) {
            totalAuto += iterator.next().get();
        }

        result.set(totalAuto);
        outputCollector.collect(text, result);
    }
}
