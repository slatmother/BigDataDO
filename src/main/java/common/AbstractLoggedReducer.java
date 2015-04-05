package common;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.log4j.Logger;

/**
 * Class:
 * Description:
 * <p/>
 * Created by: geal0913
 * Date: 05.04.2015
 */
public abstract class AbstractLoggedReducer<T, K, V, E> extends MapReduceBase implements Reducer<T, K, V, E> {
    private static final Logger LOG = Logger.getRootLogger();

    protected void infoMsg(String msg) {
        LOG.info("<" + this.getClass().toString() + "> " + msg);
    }

    protected void errorMsg(String msg) {
        LOG.error("<" + this.getClass().toString() + "> " + msg);
    }

    protected void errorMsg(Throwable e) {
        LOG.error("<" + this.getClass().toString() + "> " + e);
    }
}
