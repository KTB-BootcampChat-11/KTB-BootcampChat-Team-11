package com.ktb.chatapp.service;

import com.ktb.chatapp.model.Message;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class RedisMessageService {

    private final RedisTemplate<String, String> redisTemplate;

    private String key(String roomId) {
        return "room:messages:" + roomId;
    }

    // 최신 limit개 메시지 가져오기
    public List<String> loadRecent(String roomId, int limit) {

        Set<String> ids = redisTemplate.opsForZSet()
                .reverseRange(key(roomId), 0, limit - 1);

        if (ids == null) {
            return List.of();
        }
        return new ArrayList<>(ids);
    }

    // 특정 timestamp 이전 메시지 가져오기
    public List<String> loadBefore(String roomId, long beforeTimestamp, int limit) {

        Set<String> ids = redisTemplate.opsForZSet()
                .reverseRangeByScore(key(roomId), beforeTimestamp - 1, 0, 0, limit);

        if (ids == null) {
            return List.of();
        }
        return new ArrayList<>(ids);
    }

    // 메시지 추가
    public void addMessage(Message message) {
        long ts = message.getTimestamp().atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        redisTemplate.opsForZSet().add(key(message.getRoomId()), message.getId(), ts);
    }
}