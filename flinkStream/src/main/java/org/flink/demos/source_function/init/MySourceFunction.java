package org.flink.demos.source_function.init;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.flink.demos.source_function.init.EventLog;

import java.io.Serializable;
import java.util.HashMap;

public class MySourceFunction implements SourceFunction<EventLog>, Serializable {
    volatile boolean flag = true;

    @Override
    public void run(SourceContext<EventLog> sourceContext) throws Exception {
        EventLog eventLog = new EventLog();
        String[] events = {"appLaunch", "pageLoad", "addShow", "addClick", "itemShare", "itemCollect", "wakeUp", "appClose"};
        HashMap<String, String> eventInfoMap = new HashMap<>();
        while (flag) {
            eventLog.setGuid(RandomUtils.nextLong(1, 1000));
            eventLog.setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase());
            eventLog.setTimeStamp(System.currentTimeMillis());
            eventLog.setEventId(events[RandomUtils.nextInt(0, events.length)]);
            eventInfoMap.put(RandomStringUtils.randomAlphabetic(1), RandomStringUtils.randomAlphabetic(2));
            eventLog.setEventInfo(eventInfoMap);
            sourceContext.collect(eventLog);
            eventInfoMap.clear();

            Thread.sleep(RandomUtils.nextInt(500, 1500));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
