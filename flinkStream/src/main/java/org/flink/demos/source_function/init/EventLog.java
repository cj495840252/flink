package org.flink.demos.source_function.init;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class EventLog implements Serializable {
    private long guid;
    private String sessionId;
    private String eventId;
    private long timeStamp;
    private Map<String, String> eventInfo;
}
