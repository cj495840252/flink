package org.flink.demos.window;

import lombok.Data;

@Data
public class EventBean {
    private String id;
    private long dataTime;
    private int num;
    private String msg;

    public EventBean(String id, long dataTime, int num, String msg) {
        this.id = id;
        this.dataTime = dataTime;
        this.num = num;
        this.msg = msg;
    }

}
