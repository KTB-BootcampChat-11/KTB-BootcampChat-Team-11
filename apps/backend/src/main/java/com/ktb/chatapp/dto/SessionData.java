package com.ktb.chatapp.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionData implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 사용자 ID */
    private String userId;

    /** 세션 ID */
    private String sessionId;

    /** 생성 시간 (밀리초) */
    private long createdAt;

    /** 마지막 활동 시간 (밀리초) */
    private long lastActivity;

    /** 액세스 횟수 */
    private int accessCount;

    /** 사용자 이메일 */
    private String email;

    /** 사용자 이름 */
    private String username;

    /** 추가 메타데이터 (JSON) */
    private String metadata;

    public void incrementAccessCount() {
        this.accessCount++;
    }

    public boolean isExpired(long ttlSeconds) {
        long now = System.currentTimeMillis();
        long elapsedSeconds = (now - this.lastActivity) / 1000;
        return elapsedSeconds > ttlSeconds;
    }
}