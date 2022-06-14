package com.zlx.base.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean {
    private long guid;
    private String eventId;
    private long timeStamp;
    private String pageId;
}
