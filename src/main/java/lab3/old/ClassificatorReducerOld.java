package lab3.old;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 16.03.2015
 */
public class ClassificatorReducerOld extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
    private static final Logger LOG = Logger.getRootLogger();

    private Text result = new Text();

    @Override
    public void reduce(LongWritable key, Iterator<Text> iterator, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {
        infoMsg("Start reducer for key: [" + key.toString() + "]");

        Classificator classificator = new Classificator();

        while (iterator.hasNext()) {
            String domain = iterator.next().toString();
            classificator.addDomain(domain);
        }

        result.set(classificator.getVector());
        infoMsg("Result vector for uuid [" + key.toString() + "] is [" + result.toString() + "]");
        outputCollector.collect(key, result);
    }


    private void infoMsg(String msg) {
        LOG.info("<" + this.getClass().toString() + "> " + msg);
    }

    private void errorMsg(String msg) {
        LOG.error("<" + this.getClass().toString() + "> " + msg);
    }
}
