package vishal.flink.overspeed.filters;

import org.apache.flink.api.common.functions.FilterFunction;

public class NullFilters<T> implements FilterFunction<T> {
    @Override
    public boolean filter(T t) throws Exception {
       return t != null;
    }
}
