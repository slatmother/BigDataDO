package lab3.achieve;

import common.AbstractLoggedMapper;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 05.04.2015
 */
public class RelMapper extends AbstractLoggedMapper<LongWritable, Text, Text, IntWritable> {
    private final List<Long> autoUID = new ArrayList<Long>();
    private Text domainK = new Text();
    private IntWritable autoFlagTrue = new IntWritable(1);
    private IntWritable autoFlagFalse = new IntWritable(0);

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
                getUsersFromCache(file);
            }
        } catch (IOException e) {
            errorMsg(e);
        }
    }

    @Override
    public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
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

            domainK.set(domain);

            boolean isAutoFapper;
            try {
                Long uid = new Long(split[0]);
                isAutoFapper = autoUID.contains(uid);
            } catch (NumberFormatException e) {
                errorMsg("Not a long value: [" + split[0] + "]");
                return;
            }

            outputCollector.collect(domainK, isAutoFapper ? autoFlagTrue : autoFlagFalse);
        } else {
            infoMsg("Split array has length not 3!!!");
        }
    }

    private void getUsersFromCache(Path file) throws IOException {
            BufferedReader br = new BufferedReader(new FileReader(file.toString()));

            String line = null;
            while ((line = br.readLine()) != null) {
                autoUID.add(new Long(line.trim()));
            }

            br.close();
    }
}
