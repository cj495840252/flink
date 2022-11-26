package org.flink.demos.worldcount.databean;

public class ResultBean {
    private int id;
    private String eventId;
    private int num;
    private String s;

    public ResultBean(int id, String eventId, int num, String s) {
        this.id = id;
        this.eventId = eventId;
        this.num = num;
        this.s = s;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getS() {
        return s;
    }

    public void setS(String s) {
        this.s = s;
    }
}
