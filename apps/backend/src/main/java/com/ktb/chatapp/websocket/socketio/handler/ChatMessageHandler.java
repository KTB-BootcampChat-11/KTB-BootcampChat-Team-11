package com.ktb.chatapp.websocket.socketio.handler;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.annotation.OnEvent;
import com.ktb.chatapp.dto.ChatMessageRequest;
import com.ktb.chatapp.dto.FileResponse;
import com.ktb.chatapp.dto.MessageContent;
import com.ktb.chatapp.dto.MessageResponse;
import com.ktb.chatapp.dto.UserResponse;
import com.ktb.chatapp.model.*;
import com.ktb.chatapp.repository.FileRepository;
import com.ktb.chatapp.repository.MessageRepository;
import com.ktb.chatapp.repository.RoomRepository;
import com.ktb.chatapp.repository.UserRepository;
import com.ktb.chatapp.util.BannedWordChecker;
import com.ktb.chatapp.websocket.socketio.ai.AiService;
import com.ktb.chatapp.service.SessionService;
import com.ktb.chatapp.service.SessionValidationResult;
import com.ktb.chatapp.service.RateLimitService;
import com.ktb.chatapp.service.RateLimitCheckResult;
import com.ktb.chatapp.websocket.socketio.SocketUser;
import com.ktb.chatapp.websocket.socketio.UserRooms;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import static com.ktb.chatapp.websocket.socketio.SocketIOEvents.*;

@Slf4j
@Component
@ConditionalOnProperty(name = "socketio.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class ChatMessageHandler {
    private final SocketIOServer socketIOServer;
    private final MessageRepository messageRepository;
    private final FileRepository fileRepository;
    private final AiService aiService;
    private final SessionService sessionService;
    private final BannedWordChecker bannedWordChecker;
    private final RateLimitService rateLimitService;
    private final MeterRegistry meterRegistry;

    private final UserRooms userRooms;

    // ===== Metrics cache (핫패스에서 builder/register 반복 방지) =====
    private final ConcurrentMap<String, Timer> processingTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> messageCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> errorCounters = new ConcurrentHashMap<>();

    private Timer getProcessingTimer(String status, String messageType) {
        String key = status + ":" + messageType;
        return processingTimers.computeIfAbsent(key, k -> Timer.builder("socketio.messages.processing.time")
                .description("Socket.IO message processing time")
                .tags(Tags.of("status", status, "message_type", messageType))
                .register(meterRegistry));
    }

    private Counter getMessageCounter(String status, String messageType) {
        String key = status + ":" + messageType;
        return messageCounters.computeIfAbsent(key, k -> Counter.builder("socketio.messages.total")
                .description("Total Socket.IO messages processed")
                .tags(Tags.of("status", status, "message_type", messageType))
                .register(meterRegistry));
    }

    private Counter getErrorCounter(String errorType) {
        return errorCounters.computeIfAbsent(errorType, key -> Counter.builder("socketio.messages.errors")
                .description("Socket.IO message processing errors")
                .tag("error_type", errorType)
                .register(meterRegistry));
    }

    private void recordMessageSuccess(String messageType) {
        getMessageCounter("success", messageType).increment();
    }

    private void recordError(String errorType) {
        getErrorCounter(errorType).increment();
    }

    private void endWithError(
            Timer.Sample sample,
            String errorType,
            String messageTypeForMetric) {
        recordError(errorType);
        sample.stop(getProcessingTimer("error", messageTypeForMetric));
    }

    private void endWithSuccess(
            Timer.Sample sample,
            String messageType) {
        recordMessageSuccess(messageType);
        sample.stop(getProcessingTimer("success", messageType));
    }

    @OnEvent(CHAT_MESSAGE)
    public void handleChatMessage(SocketIOClient client, ChatMessageRequest data) {
        // 전체 처리 시간 계측 (메트릭 생성/등록은 캐시 사용)
        Timer.Sample timerSample = Timer.start(meterRegistry);
        String metricMessageType = data != null ? data.getMessageType() : "unknown";

        try {
            // 1. 기본 유효성 검사
            if (data == null) {
                client.sendEvent(ERROR, Map.of(
                        "code", "MESSAGE_ERROR",
                        "message", "메시지 데이터가 없습니다."));
                endWithError(timerSample, "null_data", metricMessageType);
                return;
            }

            var socketUser = (SocketUser) client.get("user");
            if (socketUser == null) {
                client.sendEvent(ERROR, Map.of(
                        "code", "SESSION_EXPIRED",
                        "message", "세션이 만료되었습니다. 다시 로그인해주세요."));
                endWithError(timerSample, "session_null", metricMessageType);
                return;
            }

            // 2. 세션 검증
            SessionValidationResult validation = sessionService.validateSession(socketUser.id(),
                    socketUser.authSessionId());
            if (!validation.isValid()) {
                client.sendEvent(ERROR, Map.of(
                        "code", "SESSION_EXPIRED",
                        "message", "세션이 만료되었습니다. 다시 로그인해주세요."));
                endWithError(timerSample, "session_expired", metricMessageType);
                return;
            }

            // 3. Rate limit
            RateLimitCheckResult rateLimitResult = rateLimitService.checkRateLimit(socketUser.id(), 10000,
                    Duration.ofMinutes(1));
            if (!rateLimitResult.allowed()) {
                getErrorCounter("rate_limit_exceeded").increment();
                client.sendEvent(ERROR, Map.of(
                        "code", "RATE_LIMIT_EXCEEDED",
                        "message", "메시지 전송 횟수 제한을 초과했습니다. 잠시 후 다시 시도해주세요.",
                        "retryAfter", rateLimitResult.retryAfterSeconds()));
                log.warn("Rate limit exceeded for user: {}, retryAfter: {}s",
                        socketUser.id(), rateLimitResult.retryAfterSeconds());
                endWithError(timerSample, "rate_limit", metricMessageType);
                return;
            }

            // 5. 방 접근 권한 확인
            String roomId = data.getRoom();
            if (!userRooms.isInRoom(socketUser.id(), roomId)) {
                client.sendEvent(ERROR, Map.of(
                        "code", "MESSAGE_ERROR",
                        "message", "채팅방 접근 권한이 없습니다."));
                endWithError(timerSample, "room_access_denied", metricMessageType);
                return;
            }

            // 6. 메시지 콘텐츠 파싱 / 금칙어 검사
            MessageContent messageContent = data.getParsedContent();
            log.debug("Message received - type: {}, room: {}, userId: {}, hasFileData: {}",
                    data.getMessageType(), roomId, socketUser.id(), data.hasFileData());

            if (bannedWordChecker.containsBannedWord(messageContent.getTrimmedContent())) {
                client.sendEvent(ERROR, Map.of(
                        "code", "MESSAGE_REJECTED",
                        "message", "금칙어가 포함된 메시지는 전송할 수 없습니다."));
                endWithError(timerSample, "banned_word", metricMessageType);
                return;
            }

            // 7. 메시지 생성
            String messageType = data.getMessageType();
            Message message = switch (messageType) {
                case "file" -> handleFileMessage(
                        roomId, socketUser.id(), messageContent, data.getFileData());
                case "text" -> handleTextMessage(roomId, socketUser.id(), messageContent);
                default -> throw new IllegalArgumentException("Unsupported message type: " + messageType);
            };

            if (message == null) {
                log.warn("Empty message - ignoring. room: {}, userId: {}, messageType: {}",
                        roomId, socketUser.id(), messageType);
                // 빈 메시지는 성공도/에러도 아닌 "ignored"로 태그
                timerSample.stop(getProcessingTimer("ignored", messageType));
                return;
            }

            // 8. Mongo에 메시지 저장 (핫패스 동기 구간)
            Message savedMessage = messageRepository.save(message);

            // 9. 브로드캐스트 (핫패스)
            socketIOServer.getRoomOperations(roomId)
                    .sendEvent(MESSAGE, createMessageResponse(savedMessage, socketUser));

            // 10. AI 멘션 처리 / 세션 활동 업데이트
            // - 이 부분은 추후 별도 Executor/이벤트로 비동기화하면 더 빨라질 수 있음
            aiService.handleAIMentions(roomId, socketUser.id(), messageContent);

            // 11. Metrics 성공 기록
            endWithSuccess(timerSample, messageType);

            log.debug("Message processed - messageId: {}, type: {}, room: {}",
                    savedMessage.getId(), savedMessage.getType(), roomId);

        } catch (Exception e) {
            log.error("Message handling error", e);
            client.sendEvent(ERROR, Map.of(
                    "code", "MESSAGE_ERROR",
                    "message", e.getMessage() != null ? e.getMessage() : "메시지 전송 중 오류가 발생했습니다."));
            endWithError(timerSample, "exception", metricMessageType);
        }
    }

    private Message handleFileMessage(String roomId, String userId, MessageContent messageContent,
            Map<String, Object> fileData) {
        if (fileData == null || fileData.get("_id") == null) {
            throw new IllegalArgumentException("파일 데이터가 올바르지 않습니다.");
        }

        String fileId = (String) fileData.get("_id");
        File file = fileRepository.findById(fileId).orElse(null);

        if (file == null || !file.getUser().equals(userId)) {
            throw new IllegalStateException("파일을 찾을 수 없거나 접근 권한이 없습니다.");
        }

        Message message = new Message();
        message.setRoomId(roomId);
        message.setSenderId(userId);
        message.setType(MessageType.file);
        message.setFileId(fileId);
        message.setContent(messageContent.getTrimmedContent());
        message.setTimestamp(LocalDateTime.now());
        message.setMentions(messageContent.aiMentions());

        // 메타데이터는 Map<String, Object>
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("fileType", file.getMimetype());
        metadata.put("fileSize", file.getSize());
        metadata.put("originalName", file.getOriginalname());
        message.setMetadata(metadata);

        return message;
    }

    private Message handleTextMessage(String roomId, String userId, MessageContent messageContent) {
        if (messageContent.isEmpty()) {
            return null; // 빈 메시지는 무시
        }

        Message message = new Message();
        message.setRoomId(roomId);
        message.setSenderId(userId);
        message.setContent(messageContent.getTrimmedContent());
        message.setType(MessageType.text);
        message.setTimestamp(LocalDateTime.now());
        message.setMentions(messageContent.aiMentions());

        return message;
    }

    private MessageResponse createMessageResponse(Message message, SocketUser socketUser) {
        var messageResponse = new MessageResponse();
        messageResponse.setId(message.getId());
        messageResponse.setRoomId(message.getRoomId());
        messageResponse.setContent(message.getContent());
        messageResponse.setType(message.getType());
        messageResponse.setTimestamp(message.toTimestampMillis());
        messageResponse.setReactions(message.getReactions() != null ? message.getReactions() : Collections.emptyMap());
        messageResponse.setSender(UserResponse.builder()
                .id(socketUser.id())
                .name(socketUser.name())
                .email(socketUser.email())
                .profileImage(socketUser.profileImage())
                .build());
        messageResponse.setMetadata(message.getMetadata());

        if (message.getFileId() != null) {
            fileRepository.findById(message.getFileId())
                    .ifPresent(file -> messageResponse.setFile(FileResponse.from(file)));
        }

        return messageResponse;
    }

}
