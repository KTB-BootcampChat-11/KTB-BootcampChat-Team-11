package com.ktb.chatapp.service;

import java.time.Duration;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ktb.chatapp.dto.PageRequest;
import com.ktb.chatapp.dto.RoomsResponse;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class RoomCacheService {
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    private static final Duration ROOMS_CACHE_TTL = Duration.ofSeconds(10);
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(RoomCacheService.class);

    public String buildKey(String userEmail, PageRequest pageRequest) {
        String searchKey = (pageRequest.getSearch() == null || pageRequest.getSearch().isBlank())
                ? "_"
                : pageRequest.getSearch();

        return String.format(
                "rooms:%d:%d:%s:%s:%s",
                pageRequest.getPage(),
                pageRequest.getPageSize(),
                pageRequest.getSortField(),
                pageRequest.getSortOrder(),
                searchKey);
    }

    public RoomsResponse getCachedRooms(String key) {
        String json = redisTemplate.opsForValue().get(key);
        if (json == null) {
            log.debug("rooms cache MISS: {}", key);
            return null;
        }
        try {
            log.debug("rooms cache HIT: {}", key);
            return objectMapper.readValue(json, RoomsResponse.class);
        } catch (Exception e) {
            // 파싱 실패하면 캐시 무시
            log.warn("rooms cache parse error for key {}: {}", key, e.getMessage());
            return null;
        }
    }

    public void cacheRooms(String key, RoomsResponse roomsResponse) {
        try {
            String json = objectMapper.writeValueAsString(roomsResponse);
            redisTemplate.opsForValue().set(key, json, ROOMS_CACHE_TTL);
            log.debug("rooms cache SET: {}", key);
        } catch (Exception e) {
            // 캐싱 실패해도 서비스는 계속 진행
            log.warn("rooms cache SET fail for key {}: {}", key, e.getMessage());
        }
    }

    // 방 생성/삭제/참여 등에서 캐시를 비우고 싶다면 여기에 메서드 추가
    public void evictAllRoomsCacheForUser(String userEmail) {
        String pattern = "rooms:" + "*";
        // keys 사용은 규모 커지면 주의, 지금은 간단히 예시
        var keys = redisTemplate.keys(pattern);
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
    }
}
