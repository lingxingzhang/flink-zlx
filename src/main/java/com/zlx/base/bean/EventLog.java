package com.zlx.base.bean;

import lombok.*;

import java.util.Map;

@NoArgsConstructor
@AllArgsConstructor
@Data
@ToString
@Builder
public class EventLog {
    private Long guid;
    private String sessionId;
    private String eventId;
    private Long timeStamp;
    private Map<String,String> eventInfo;
}