package com.ktb.chatapp.websocket.socketio;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Primary
@RequiredArgsConstructor
public class RedisChatDataStore implements ChatDataStore {

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public <T> Optional<T> get(String key, Class<T> type) {
        try {
            String json = stringRedisTemplate.opsForValue().get(key);
            if (json == null) {
                return Optional.empty();
            }
            T value = objectMapper.readValue(json, type);
            return Optional.of(value);
        } catch (Exception e) {
            log.error("Failed to get value from Redis. key={}, type={}", key, type.getName(), e);
            return Optional.empty();
        }
    }

    @Override
    public void set(String key, Object value) {
        try {
            String json = objectMapper.writeValueAsString(value);
            stringRedisTemplate.opsForValue().set(key, json);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize value for Redis. key={}, valueClass={}",
                    key, value != null ? value.getClass().getName() : "null", e);
            // 필요하면 여기서 런타임 예외를 던져도 됨
        }
    }

    @Override
    public void delete(String key) {
        try {
            stringRedisTemplate.delete(key);
        } catch (Exception e) {
            log.error("Failed to delete key from Redis. key={}", key, e);
        }
    }

    @Override
    public int size() {
        try {
            Long dbSize = stringRedisTemplate.execute(RedisServerCommands::dbSize);
            return dbSize != null ? dbSize.intValue() : 0;
        } catch (Exception e) {
            log.error("Failed to get Redis DB size", e);
            return 0;
        }
    }
}
