package org.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Message {
    private String id;
    private String payload;
    private Long createdTime;

    public Message(String message, Long createdTime) {
        this.id = UUID.randomUUID().toString();
        this.payload = message;
        this.createdTime = createdTime;
    }
}