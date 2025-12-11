package com.ktb.chatapp.service.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ktb.chatapp.model.Session;
import java.time.Duration;
import java.util.Optional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.convert.DurationStyle;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import static com.ktb.chatapp.model.Session.SESSION_TTL;

@Slf4j
@Component
@Primary
@RequiredArgsConstructor
public class RedisSessionStore implements SessionStore {

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    public static final long SESSION_TTL_SEC = DurationStyle.detectAndParse(SESSION_TTL).getSeconds();

    private String buildKey(String userId) {
        return "session:" + userId;
    }

    @Override
    public Session save(Session session) {
        try {
            String key = buildKey(session.getUserId());
            String value = objectMapper.writeValueAsString(session);

            // log.info("[RedisSessionStore] SAVE session. key={}, ttlSec={}, userId={},
            // sessionId={}",
            // key, SESSION_TTL_SEC, session.getUserId(), session.getSessionId());

            stringRedisTemplate.opsForValue()
                    .set(key, value, Duration.ofSeconds(SESSION_TTL_SEC));

            return session;

        } catch (Exception e) {
            // log.error("[RedisSessionStore] ERROR saving session. userId={}",
            // session.getUserId(), e);
            throw new RuntimeException("세션 저장 중 오류가 발생했습니다.", e);
        }
    }

    @Override
    public Optional<Session> findByUserId(String userId) {
        try {
            String key = buildKey(userId);
            String json = stringRedisTemplate.opsForValue().get(key);

            // log.info("[RedisSessionStore] FIND session. key={}, exists={}",
            // key, json != null);

            if (json == null) {
                return Optional.empty();
            }

            Session session = objectMapper.readValue(json, Session.class);
            return Optional.of(session);

        } catch (Exception e) {
            // log.error("[RedisSessionStore] ERROR finding session. userId={}", userId, e);
            return Optional.empty();
        }
    }

    @Override
    public void delete(String userId, String sessionId) {
        String key = buildKey(userId);

        // log.info("[RedisSessionStore] DELETE session. key={}, sessionId={}",
        // key, sessionId);

        stringRedisTemplate.delete(key);
    }

    @Override
    public void deleteAll(String userId) {
        String key = buildKey(userId);

        // log.info("[RedisSessionStore] DELETE ALL sessions. key={}", key);

        stringRedisTemplate.delete(key);
    }
}
