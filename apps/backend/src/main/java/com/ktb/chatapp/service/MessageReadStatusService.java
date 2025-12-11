package com.ktb.chatapp.service;

import com.ktb.chatapp.model.Message;
import com.ktb.chatapp.repository.MessageRepository;
import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

/**
 * 메시지 읽음 상태 관리 서비스 (고성능 버전)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageReadStatusService {

    private final MongoTemplate mongoTemplate;

    /**
     * 메시지 읽음 상태 업데이트 (MongoDB updateMany 기반)
     *
     * @param messageIds 읽음 처리할 메시지 ID 리스트
     * @param userId     읽은 사용자 ID
     */
    public void updateReadStatus(List<String> messageIds, String userId) {
        if (messageIds == null || messageIds.isEmpty()) {
            return;
        }

        LocalDateTime readAt = LocalDateTime.now();

        try {
            Message.MessageReader readerInfo = Message.MessageReader.builder()
                    .userId(userId)
                    .readAt(readAt)
                    .build();

            Query query = new Query(
                    Criteria.where("_id").in(messageIds)
                            .and("readers.userId").ne(userId) // 이미 읽은 메시지는 제외
            );

            Update update = new Update()
                    .addToSet("readers", readerInfo);

            var result = mongoTemplate.updateMulti(query, update, Message.class);

            log.debug("Read status updated for {} messages by user {}",
                    result.getModifiedCount(), userId);

        } catch (Exception e) {
            log.error("Read status update error for user {}", userId, e);
        }
    }
}