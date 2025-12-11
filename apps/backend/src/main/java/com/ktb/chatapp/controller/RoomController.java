package com.ktb.chatapp.controller;

import com.ktb.chatapp.annotation.RateLimit;
import com.ktb.chatapp.dto.*;
import com.ktb.chatapp.model.Room;
import com.ktb.chatapp.model.User;
import com.ktb.chatapp.repository.MessageRepository;
import com.ktb.chatapp.repository.UserRepository;
import com.ktb.chatapp.service.RoomCacheService;
import com.ktb.chatapp.service.RoomService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.security.Principal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Tag(name = "채팅방 (Rooms)", description = "채팅방 생성 및 관리 API - 채팅방 목록 조회, 생성, 참여, 헬스체크")
@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/rooms")
public class RoomController {

    private final UserRepository userRepository;
    private final MessageRepository messageRepository;
    private final RoomService roomService;
    private final RoomCacheService roomCacheService;

    @Value("${spring.profiles.active:production}")
    private String activeProfile;

    // =====================================================================
    // Health Check
    // =====================================================================
    @Operation(summary = "채팅방 서비스 헬스체크", description = "채팅방 서비스의 상태를 확인합니다.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "서비스 정상", content = @Content(schema = @Schema(implementation = HealthResponse.class))),
            @ApiResponse(responseCode = "503", description = "서비스 사용 불가", content = @Content(schema = @Schema(implementation = HealthResponse.class)))
    })
    @SecurityRequirement(name = "")
    @GetMapping("/health")
    public ResponseEntity<HealthResponse> healthCheck() {
        try {
            HealthResponse healthResponse = roomService.getHealthStatus();

            return ResponseEntity
                    .status(healthResponse.isSuccess() ? 200 : 503)
                    .cacheControl(CacheControl.noCache().mustRevalidate())
                    .header("Pragma", "no-cache")
                    .header("Expires", "0")
                    .body(healthResponse);

        } catch (Exception e) {
            log.error("Health check 에러", e);

            HealthResponse errorResponse = HealthResponse.builder()
                    .success(false)
                    .build();

            return ResponseEntity.status(503)
                    .cacheControl(CacheControl.noCache())
                    .body(errorResponse);
        }
    }

    // =====================================================================
    // 채팅방 목록 조회 (캐시 + RateLimit)
    // =====================================================================
    @Operation(summary = "채팅방 목록 조회", description = "페이지네이션과 검색 기능이 적용된 채팅방 목록을 조회합니다.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "채팅방 목록 조회 성공"),
            @ApiResponse(responseCode = "401", description = "인증 실패"),
            @ApiResponse(responseCode = "429", description = "요청 한도 초과"),
            @ApiResponse(responseCode = "500", description = "서버 내부 오류")
    })
    @GetMapping
    @RateLimit
    public ResponseEntity<?> getAllRooms(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int pageSize,
            @RequestParam(defaultValue = "createdAt") String sortField,
            @RequestParam(defaultValue = "desc") String sortOrder,
            @RequestParam(required = false) String search,
            Principal principal) {

        try {
            PageRequest pageRequest = new PageRequest();
            pageRequest.setPage(Math.max(0, page));
            pageRequest.setPageSize(Math.min(Math.max(1, pageSize), 50));
            pageRequest.setSortField(sortField);
            pageRequest.setSortOrder(sortOrder);
            pageRequest.setSearch(search);

            String userEmail = principal.getName();
            String cacheKey = roomCacheService.buildKey(userEmail, pageRequest);

            RoomsResponse cached = roomCacheService.getCachedRooms(cacheKey);
            if (cached != null) {
                return ResponseEntity.ok()
                        .cacheControl(CacheControl.maxAge(Duration.ofSeconds(10)))
                        .header("X-Cache", "hit")
                        .body(cached);
            }

            RoomsResponse response = roomService.getAllRoomsWithPagination(pageRequest, userEmail);
            roomCacheService.cacheRooms(cacheKey, response);

            return ResponseEntity.ok()
                    .cacheControl(CacheControl.maxAge(Duration.ofSeconds(10)))
                    .header("X-Cache", "miss")
                    .body(response);

        } catch (Exception e) {
            log.error("방 목록 조회 에러", e);
            return ResponseEntity.status(500)
                    .body(new ErrorResponse(false, "채팅방 목록을 불러오는데 실패했습니다."));
        }
    }

    // =====================================================================
    // 채팅방 생성 — Light Version 사용!
    // =====================================================================
    @Operation(summary = "채팅방 생성", description = "새로운 채팅방 생성 API")
    @PostMapping
    public ResponseEntity<?> createRoom(@Valid @RequestBody CreateRoomRequest createRoomRequest,
                                        Principal principal) {
        try {
            if (createRoomRequest.getName() == null || createRoomRequest.getName().trim().isEmpty()) {
                return ResponseEntity.status(400).body(StandardResponse.error("방 이름은 필수입니다."));
            }

            Room savedRoom = roomService.createRoom(createRoomRequest, principal.getName());

            // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            // Light Version 사용 (DB 조회 없음 → 즉시 응답)
            // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            RoomResponse roomResponse = mapToRoomResponseLight(savedRoom, principal.getName());

            return ResponseEntity.status(201).body(Map.of(
                    "success", true,
                    "data", roomResponse
            ));

        } catch (Exception e) {
            log.error("방 생성 에러", e);
            String errorMessage = "채팅방 생성에 실패했습니다.";

            if ("development".equals(activeProfile)) {
                errorMessage += " (" + e.getMessage() + ")";
            }

            return ResponseEntity.status(500)
                    .body(StandardResponse.error(errorMessage));
        }
    }

    // =====================================================================
    // 채팅방 상세 조회 — Full Version 사용
    // =====================================================================
    @GetMapping("/{roomId}")
    public ResponseEntity<?> getRoomById(@PathVariable String roomId, Principal principal) {
        try {
            Optional<Room> roomOpt = roomService.findRoomById(roomId);
            if (roomOpt.isEmpty()) {
                return ResponseEntity.status(404)
                        .body(StandardResponse.error("채팅방을 찾을 수 없습니다."));
            }

            RoomResponse roomResponse = mapToRoomResponseFull(roomOpt.get(), principal.getName());

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "data", roomResponse
            ));

        } catch (Exception e) {
            log.error("채팅방 조회 에러", e);
            return ResponseEntity.status(500)
                    .body(StandardResponse.error("채팅방 정보를 불러오는데 실패했습니다."));
        }
    }

    // =====================================================================
    // 채팅방 참여 — Full Version 사용
    // =====================================================================
    @PostMapping("/{roomId}/join")
    public ResponseEntity<?> joinRoom(@PathVariable String roomId,
                                      @RequestBody JoinRoomRequest joinRoomRequest,
                                      Principal principal) {
        try {
            Room joinedRoom = roomService.joinRoom(roomId, joinRoomRequest.getPassword(), principal.getName());

            if (joinedRoom == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(StandardResponse.error("채팅방을 찾을 수 없습니다."));
            }

            RoomResponse roomResponse = mapToRoomResponseFull(joinedRoom, principal.getName());

            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "data", roomResponse
            ));

        } catch (RuntimeException e) {
            if (e.getMessage().contains("비밀번호")) {
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                        .body(StandardResponse.error("비밀번호가 일치하지 않습니다."));
            }
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(StandardResponse.error(e.getMessage()));
        } catch (Exception e) {
            log.error("채팅방 참여 에러", e);
            return ResponseEntity.status(500)
                    .body(StandardResponse.error("채팅방 참여에 실패했습니다."));
        }
    }

    // =====================================================================
    // Light Version — 채팅방 생성 시 초고속 응답용
    // =====================================================================
    private RoomResponse mapToRoomResponseLight(Room room, String creatorEmail) {

        UserResponse creator = UserResponse.builder()
                .id(room.getCreator())
                .email(room.getCreator())
                .name(room.getCreator())
                .profileImage(null)
                .build();

        return RoomResponse.builder()
                .id(room.getId())
                .name(room.getName())
                .hasPassword(room.isHasPassword())
                .creator(creator)
                .participants(List.of(creator))
                .createdAtDateTime(room.getCreatedAt())
                .isCreator(true)
                .recentMessageCount(0)
                .build();
    }

    // =====================================================================
    // Full Version — DB 조회 포함 (상세 / 참여 / 목록용)
    // =====================================================================
    private RoomResponse mapToRoomResponseFull(Room room, String currentUserEmail) {

        List<String> participantIds = room.getParticipantIds() != null
                ? room.getParticipantIds().stream().sorted().toList()
                : Collections.emptyList();

        Set<String> userIds = new HashSet<>();
        if (room.getCreator() != null) userIds.add(room.getCreator());
        userIds.addAll(participantIds);

        Map<String, User> userMap = userIds.isEmpty()
                ? Collections.emptyMap()
                : userRepository.findAllById(userIds).stream()
                .collect(Collectors.toMap(User::getId, u -> u));

        User creator = userMap.get(room.getCreator());
        if (creator == null) {
            throw new IllegalStateException("Creator not found");
        }

        UserResponse creatorSummary = UserResponse.from(creator);

        List<UserResponse> participantSummaries = participantIds.stream()
                .map(userMap::get)
                .filter(Objects::nonNull)
                .map(UserResponse::from)
                .toList();

        boolean isCreator = room.getCreator().equals(currentUserEmail);

        LocalDateTime tenMinutesAgo = LocalDateTime.now().minusMinutes(10);
        long recentMessageCount =
                messageRepository.countRecentMessagesByRoomId(room.getId(), tenMinutesAgo);

        return RoomResponse.builder()
                .id(room.getId())
                .name(room.getName())
                .hasPassword(room.isHasPassword())
                .creator(creatorSummary)
                .participants(participantSummaries)
                .createdAtDateTime(room.getCreatedAt())
                .isCreator(isCreator)
                .recentMessageCount((int) recentMessageCount)
                .build();
    }
}