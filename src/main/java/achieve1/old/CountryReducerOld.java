package achieve1.old;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 16.03.2015
 */
public class CountryReducerOld extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int totalClicks = 0;

        while (iterator.hasNext()) {
            totalClicks += iterator.next().get();
        }

        result.set(totalClicks);
        outputCollector.collect(key, result);
    }
}
