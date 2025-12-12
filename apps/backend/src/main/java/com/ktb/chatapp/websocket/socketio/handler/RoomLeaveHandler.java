package com.ktb.chatapp.websocket.socketio.handler;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.annotation.OnEvent;
import com.ktb.chatapp.dto.MessageResponse;
import com.ktb.chatapp.dto.UserResponse;
import com.ktb.chatapp.model.Message;
import com.ktb.chatapp.model.MessageType;
import com.ktb.chatapp.model.Room;
import com.ktb.chatapp.repository.MessageRepository;
import com.ktb.chatapp.repository.RoomRepository;
import com.ktb.chatapp.repository.UserRepository;
import com.ktb.chatapp.websocket.socketio.SocketUser;
import com.ktb.chatapp.websocket.socketio.UserRooms;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import static com.ktb.chatapp.websocket.socketio.SocketIOEvents.*;

@Slf4j
@Component
@ConditionalOnProperty(name = "socketio.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class RoomLeaveHandler {

    private final SocketIOServer socketIOServer;
    private final MessageRepository messageRepository;
    private final RoomRepository roomRepository;
    private final UserRepository userRepository;
    private final UserRooms userRooms;
    private final MessageResponseMapper messageResponseMapper;

    @OnEvent(LEAVE_ROOM)
    public void handleLeaveRoom(SocketIOClient client, String roomId) {
        try {
            String userId = getUserId(client);
            String userName = getUserName(client);

            if (userId == null) {
                client.sendEvent(ERROR, Map.of("message", "Unauthorized"));
                return;
            }

            if (!userRooms.isInRoom(userId, roomId)) {
                log.debug("User {} is not in room {}", userId, roomId);
                return;
            }

            // 🔥 User 조회 제거, Room만 조회
            Room room = roomRepository.findById(roomId).orElse(null);
            if (room == null) {
                log.warn("Room {} not found for user {}", roomId, userId);
                return;
            }

            // DB에서 참가자 제거
            roomRepository.removeParticipant(roomId, userId);

            // 소켓/메모리 상태 정리
            client.leaveRoom(roomId);
            userRooms.remove(userId, roomId);

            // 메모리 상 Room 객체도 동기화 (broadcast에서 재사용)
            if (room.getParticipantIds() != null) {
                room.getParticipantIds().remove(userId);
            }

            log.info("User {} left room {}", userName, room.getName());
            log.debug("Leave room cleanup - roomId: {}, userId: {}", roomId, userId);

            // 시스템 메시지 브로드캐스트
            sendSystemMessage(roomId, userName + "님이 퇴장하였습니다.");

            // 참가자 목록 브로드캐스트 (room 재조회 X, N+1 X)
            broadcastParticipantList(room);

            // 유저 퇴장 이벤트
            socketIOServer.getRoomOperations(roomId)
                    .sendEvent(USER_LEFT, Map.of(
                            "userId", userId,
                            "userName", userName));

        } catch (Exception e) {
            log.error("Error handling leaveRoom", e);
            client.sendEvent(ERROR, Map.of("message", "채팅방 퇴장 중 오류가 발생했습니다."));
        }
    }

    private void sendSystemMessage(String roomId, String content) {
        try {
            Message systemMessage = new Message();
            systemMessage.setRoomId(roomId);
            systemMessage.setContent(content);
            systemMessage.setType(MessageType.system);
            systemMessage.setTimestamp(LocalDateTime.now());
            systemMessage.setMentions(new ArrayList<>());
            systemMessage.setIsDeleted(false);
            systemMessage.setReactions(new HashMap<>());
            systemMessage.setReaders(new ArrayList<>());
            systemMessage.setMetadata(new HashMap<>());

            Message savedMessage = messageRepository.save(systemMessage);
            MessageResponse response = messageResponseMapper.mapToMessageResponse(savedMessage, null);

            socketIOServer.getRoomOperations(roomId)
                    .sendEvent(MESSAGE, response);

        } catch (Exception e) {
            log.error("Error sending system message", e);
        }
    }

    // 🔥 Room 재조회 하지 않고, 이미 있는 Room 객체 사용 + findAllById 한 번만
    private void broadcastParticipantList(Room room) {
        Set<String> participantIds = room.getParticipantIds();
        if (participantIds == null || participantIds.isEmpty()) {
            // 아무도 없으면 브로드캐스트 안 해도 됨
            return;
        }

        var users = userRepository.findAllById(participantIds);
        var participantList = users.stream()
                .map(UserResponse::from)
                .toList();

        if (participantList.isEmpty()) {
            return;
        }

        socketIOServer.getRoomOperations(room.getId())
                .sendEvent(PARTICIPANTS_UPDATE, participantList);
    }

    private SocketUser getUserDto(SocketIOClient client) {
        return client.get("user");
    }

    private String getUserId(SocketIOClient client) {
        SocketUser user = getUserDto(client);
        return user != null ? user.id() : null;
    }

    private String getUserName(SocketIOClient client) {
        SocketUser user = getUserDto(client);
        return user != null ? user.name() : null;
    }
}
