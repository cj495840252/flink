package org.flink.demos.window;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;



public class MyEvictor extends TimeEvictor<TimeWindow> {
    public static final Logger LOGGER = LoggerFactory.getLogger(MyEvictor.class);
    private final long size;// 窗口长度
    private boolean isAfter = false;// 判断窗口出发前后的操作
    private String flagEvent; // 用来标记什么样的数据需要移除
    public MyEvictor(long windowSize) {
        super(windowSize);
        this.size=windowSize;
    }

    public MyEvictor(long windowSize, boolean doEvictAfter) {
        super(windowSize, doEvictAfter);
        this.size=windowSize;
        this.isAfter=doEvictAfter;
    }

    public MyEvictor(long windowSize, boolean doEvictAfter,String flagEvent) {
        super(windowSize, doEvictAfter);
        this.size=windowSize;
        this.isAfter=doEvictAfter;
        this.flagEvent=flagEvent;
    }

    /**
     * 触发计算前，数据的操作
     * @param elements
     * @param size
     * @param window
     * @param ctx
     */
    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, Evictor.EvictorContext ctx) {
        if (!this.isAfter) {
            LOGGER.warn(String.valueOf(size));
            this.Myevictor(elements, size, ctx);
        }
    }

    /**
     * 触发计算后的操作
     * @param elements
     * @param size
     * @param window
     * @param ctx
     */
    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, Evictor.EvictorContext ctx) {
        if (this.isAfter) {
            this.Myevictor(elements, size, ctx);
        }
    }
    private void Myevictor(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
        if (this.hasTimestamp(elements)) {
            long currentTime = getMaxTimestamp(elements);
            long evictCutoff = currentTime - this.size;
            Iterator<TimestampedValue<Object>> iterator = elements.iterator();

            while(iterator.hasNext()) {
                TimestampedValue<Object> record = (TimestampedValue)iterator.next();
                // 取出数据强转，移除msg = click数据
                EventBean value = (EventBean)record.getValue();
                if (record.getTimestamp() <= evictCutoff || flagEvent.equals(value.getMsg())) {
                    iterator.remove();
                }

            }

        }
    }

    private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {

        Iterator<TimestampedValue<Object>> it = elements.iterator();
        return it.hasNext() ? ((TimestampedValue)it.next()).hasTimestamp() : false;
    }

    private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
        long currentTime = Long.MIN_VALUE;

        TimestampedValue record;
        for(Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); currentTime = Math.max(currentTime, record.getTimestamp())) {
            record = (TimestampedValue)iterator.next();
        }

        return currentTime;
    }


}
