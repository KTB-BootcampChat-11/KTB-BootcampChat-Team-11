package com.ktb.chatapp.websocket.socketio.handler;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.annotation.OnEvent;
import com.ktb.chatapp.dto.MarkAsReadRequest;
import com.ktb.chatapp.dto.MessagesReadResponse;
import com.ktb.chatapp.model.Message;
import com.ktb.chatapp.repository.MessageRepository;
import com.ktb.chatapp.service.MessageReadStatusService;
import com.ktb.chatapp.websocket.socketio.SocketUser;
import com.ktb.chatapp.websocket.socketio.UserRooms;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import static com.ktb.chatapp.websocket.socketio.SocketIOEvents.*;

@Slf4j
@Component
@ConditionalOnProperty(name = "socketio.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class MessageReadHandler {

    private final SocketIOServer socketIOServer;
    private final MessageReadStatusService messageReadStatusService;
    private final MessageRepository messageRepository;
    private final UserRooms userRooms; // ✅ DB 말고 이걸로 방 접근 체크

    @OnEvent(MARK_MESSAGES_AS_READ)
    public void handleMarkAsRead(SocketIOClient client, MarkAsReadRequest data) {
        try {
            SocketUser socketUser = (SocketUser) client.get("user");
            if (socketUser == null) {
                client.sendEvent(ERROR, Map.of("message", "Unauthorized"));
                return;
            }
            String userId = socketUser.id();

            if (data == null || data.getMessageIds() == null || data.getMessageIds().isEmpty()) {
                // 굳이 에러 보낼 필요는 없고, 그냥 무시
                return;
            }

            List<String> messageIds = data.getMessageIds();
            String firstMessageId = messageIds.get(0);

            // ✅ roomId 얻기 위해 딱 한 번만 메시지 조회
            Message firstMessage = messageRepository.findById(firstMessageId).orElse(null);
            if (firstMessage == null) {
                client.sendEvent(ERROR, Map.of("message", "Invalid message"));
                return;
            }

            String roomId = firstMessage.getRoomId();
            if (roomId == null || roomId.isBlank()) {
                client.sendEvent(ERROR, Map.of("message", "Invalid room"));
                return;
            }

            // ✅ DB 대신 UserRooms 로 방 참여 여부 확인
            if (!userRooms.isInRoom(userId, roomId)) {
                client.sendEvent(ERROR, Map.of("message", "Room access denied"));
                return;
            }

            // ✅ 여기서 MongoTemplate updateMulti 한 방
            messageReadStatusService.updateReadStatus(messageIds, userId);

            MessagesReadResponse response = new MessagesReadResponse(userId, messageIds);

            // 방 전체에 브로드캐스트
            socketIOServer.getRoomOperations(roomId)
                    .sendEvent(MESSAGES_READ, response);

        } catch (Exception e) {
            log.error("Error handling markMessagesAsRead", e);
            client.sendEvent(ERROR, Map.of(
                    "message", "읽음 상태 업데이트 중 오류가 발생했습니다."));
        }
    }
}
