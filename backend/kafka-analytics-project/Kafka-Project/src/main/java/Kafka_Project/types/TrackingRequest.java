package Kafka_Project.types;

import java.util.Map;

public class TrackingRequest {
    private String userId;
    private String event;
    private Map<String, Object> data;
    private Long timestamp;
    
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    
    public String getEvent() { return event; }
    public void setEvent(String event) { this.event = event; }
    
    public Map<String, Object> getData() { return data; }
    public void setData(Map<String, Object> data) { this.data = data; }
    
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
}