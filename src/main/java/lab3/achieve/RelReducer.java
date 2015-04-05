package lab3.achieve;

import common.AbstractLoggedReducer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 05.04.2015
 */
public class RelReducer extends AbstractLoggedReducer<Text, Text, Text, Text> {
    private Long totalAutos;
    private Text result = new Text();

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        try {
            Path[] cache = DistributedCache.getLocalCacheFiles(job);

            if (cache == null) {
                throw new IllegalArgumentException("Distributed cache is null!!");
            } else {
                infoMsg("Distributed cache contains URI: " + Arrays.asList(cache));
                Path file = cache[0];
                getTotalAutoLoversFromCache(file);
            }
        } catch (IOException e) {
            errorMsg(e);
        }
    }

    private void getTotalAutoLoversFromCache(Path file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file.toString()));

        String line = br.readLine();
        totalAutos = new Long(line.trim().split("\t")[1]);

        br.close();
    }

    @Override
    public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        int domainAutoProb = 0;
        int domainVisits = 0;

        while (iterator.hasNext()) {
            domainAutoProb += new Integer(iterator.next().toString());
            domainVisits++;
        }

        Double rel = Math.pow(domainAutoProb, 2) / (domainVisits * totalAutos);

        DecimalFormat df = new DecimalFormat("0.0000000000");
        result.set(df.format(rel));

        outputCollector.collect(text, result);
    }
}
