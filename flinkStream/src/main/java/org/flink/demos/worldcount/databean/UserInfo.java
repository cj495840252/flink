package org.flink.demos.worldcount.databean;

import java.util.Objects;

public class UserInfo {
    private int id;
    private String eventId;
    private int num;

    public UserInfo() {
    }

    public UserInfo(int id, String eventId, int num) {
        this.id = id;
        this.eventId = eventId;
        this.num = num;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserInfo userInfo = (UserInfo) o;
        return id == userInfo.id && eventId == userInfo.eventId && num == userInfo.num;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, eventId, num);
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "id=" + id +
                ", eventId=" + eventId +
                ", num=" + num +
                '}';
    }
}
