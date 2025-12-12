package com.ktb.chatapp.websocket.socketio.handler;

import com.ktb.chatapp.dto.FetchMessagesRequest;
import com.ktb.chatapp.dto.FetchMessagesResponse;
import com.ktb.chatapp.dto.MessageResponse;
import com.ktb.chatapp.model.Message;
import com.ktb.chatapp.model.User;
import com.ktb.chatapp.repository.MessageRepository;
import com.ktb.chatapp.repository.UserRepository;
import com.ktb.chatapp.service.MessageReadStatusService;
import com.ktb.chatapp.service.RedisMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessageLoader {

    private final MessageRepository messageRepository;
    private final UserRepository userRepository;
    private final MessageResponseMapper messageResponseMapper;
    private final MessageReadStatusService messageReadStatusService;
    private final RedisMessageService redisMessageService;

    private static final int BATCH_SIZE = 30;

    public FetchMessagesResponse loadMessages(FetchMessagesRequest data, String userId) {

        LocalDateTime before = data.before(LocalDateTime.now());
        long beforeTs = before.toInstant(ZoneOffset.UTC).toEpochMilli();

        // 1. Redis ZSET에서 메시지 ID 가져오기
        List<String> cachedIds = redisMessageService.loadBefore(
                data.roomId(), beforeTs, data.limit(BATCH_SIZE));

        List<Message> messages;

        if (cachedIds.size() == data.limit(BATCH_SIZE)) {
            // 2. DB 조회 없이 messageId → DB findAllById
            messages = messageRepository.findAllById(cachedIds);
        } else {
            // 3. 부족한 메시지만 DB에서 조회
            int remaining = data.limit(BATCH_SIZE) - cachedIds.size();

            Pageable pageable = PageRequest.of(0, remaining,
                    Sort.by(Sort.Direction.DESC, "timestamp"));

            Page<Message> page = messageRepository.findByRoomIdAndIsDeletedAndTimestampBefore(
                    data.roomId(), false, before, pageable
            );

            List<Message> dbMessages = page.getContent();

            // DB 메시지 → Redis에 추가
            dbMessages.forEach(redisMessageService::addMessage);

            // Redis 캐시와 DB 메시지를 합치기
            messages = new ArrayList<>(messageRepository.findAllById(cachedIds));
            messages.addAll(dbMessages);

            messages.sort(Comparator.comparing(Message::getTimestamp));
        }

        // 읽음 처리
        List<String> messageIds = messages.stream().map(Message::getId).toList();
        messageReadStatusService.updateReadStatus(messageIds, userId);

        // user 정보 로딩
        Set<String> senderIds = messages.stream()
                .map(Message::getSenderId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        Map<String, User> userMap = userRepository.findAllById(senderIds)
                .stream()
                .collect(Collectors.toMap(User::getId, Function.identity()));

        // Response 매핑
        List<MessageResponse> responses = messages.stream()
                .map(m -> messageResponseMapper.mapToMessageResponse(m, userMap.get(m.getSenderId())))
                .toList();

        return FetchMessagesResponse.builder()
                .messages(responses)
                .hasMore(responses.size() == data.limit(BATCH_SIZE))
                .build();
    }
}