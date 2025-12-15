import { useState, useCallback, useMemo } from 'react';
import { Toast } from '../components/Toast';
import fileService from '../services/fileService';

export const useMessageHandling = (
  socketRef,
  currentUser,
  router,
  handleSessionError,
  messages = [],
  loadingMessages = false,
  setLoadingMessages
) => {
  const [message, setMessage] = useState('');
  const [showEmojiPicker, setShowEmojiPicker] = useState(false);
  const [showMentionList, setShowMentionList] = useState(false);
  const [mentionFilter, setMentionFilter] = useState('');
  const [mentionIndex, setMentionIndex] = useState(0);
  const [filePreview, setFilePreview] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadError, setUploadError] = useState(null);

  // router.query 전체 대신 roomId만 뽑아서 사용 (deps 안정화)
  const roomId = router?.query?.room;

  /**
   * 가장 오래된 메시지 타임스탬프 계산
   * - 매번 sort/new Date 하는 대신, O(n) 스캔 + useMemo
   * - messages가 클 경우 효과 큼
   */
  const oldestTimestamp = useMemo(() => {
    if (!messages || messages.length === 0) return null;

    // timestamp가 숫자/ISO 문자열 둘 다라고 가정하고 비교
    let oldest = messages[0];

    for (let i = 1; i < messages.length; i += 1) {
      const current = messages[i];
      if (!current?.timestamp) continue;
      if (!oldest?.timestamp || current.timestamp < oldest.timestamp) {
        oldest = current;
      }
    }

    return oldest?.timestamp ?? null;
  }, [messages]);

  /**
   * 메시지 입력 변경 핸들러
   * - @ 뒤에 띄어쓰기 나오기 전까지 mention 모드
   */
  const handleMessageChange = useCallback((e) => {
    const newValue = e.target.value;
    setMessage(newValue);

    const cursorPosition = e.target.selectionStart ?? newValue.length;
    const textBeforeCursor = newValue.slice(0, cursorPosition);
    const atSymbolIndex = textBeforeCursor.lastIndexOf('@');

    if (atSymbolIndex !== -1) {
      const mentionText = textBeforeCursor.slice(atSymbolIndex + 1);
      if (!mentionText.includes(' ')) {
        setMentionFilter(mentionText.toLowerCase());
        setShowMentionList(true);
        setMentionIndex(0);
        return;
      }
    }

    setShowMentionList(false);
  }, []);

  /**
   * 이전 메시지 더 불러오기
   * - 정렬/Date 생성 제거
   * - 이미 로딩 중이거나 소켓 끊기면 noop
   */
  const handleLoadMore = useCallback(() => {
    const socket = socketRef.current;

    if (!socket?.connected) return;
    if (loadingMessages) return;
    if (!roomId || !oldestTimestamp) return;

    setLoadingMessages(true);

    socket.emit('fetchPreviousMessages', {
      roomId,
      before: oldestTimestamp,
      limit: 30,
    });
  }, [socketRef, roomId, oldestTimestamp, loadingMessages, setLoadingMessages]);

  /**
   * 메시지 전송 (텍스트/파일)
   */
  const handleMessageSubmit = useCallback(
    async (messageData) => {
      if (!socketRef.current?.connected || !currentUser) {
        Toast.error('채팅 서버와 연결이 끊어졌습니다.');
        return;
      }

      if (!roomId) {
        Toast.error('채팅방 정보를 찾을 수 없습니다.');
        return;
      }

      try {
        if (messageData.type === 'file') {
          // 이미 업로드 중이면 중복 업로드 방지
          if (uploading) return;

          setUploading(true);
          setUploadError(null);
          setUploadProgress(0);

          const uploadResponse = await fileService.uploadFile(
            messageData.fileData.file,
            (progress) => setUploadProgress(progress)
          );

          if (!uploadResponse.success) {
            throw new Error(uploadResponse.message || '파일 업로드에 실패했습니다.');
          }

          const file = uploadResponse.data?.file;

          socketRef.current.emit('chatMessage', {
            room: roomId,
            type: 'file',
            content: messageData.content || '',
            fileData: {
              _id: file._id,
              filename: file.filename,
              originalname: file.originalname,
              mimetype: file.mimetype,
              size: file.size,
            },
          });

          setFilePreview(null);
          setMessage('');
          setUploading(false);
          setUploadProgress(0);
        } else {
          const trimmed = messageData.content?.trim();
          if (!trimmed) return;

          socketRef.current.emit('chatMessage', {
            room: roomId,
            type: 'text',
            content: trimmed,
          });

          setMessage('');
        }

        setShowEmojiPicker(false);
        setShowMentionList(false);
      } catch (error) {
        const msg = error?.message || '메시지 전송 중 오류가 발생했습니다.';

        if (
          msg.includes('세션') ||
          msg.includes('인증') ||
          msg.includes('토큰')
        ) {
          await handleSessionError();
          return;
        }

        Toast.error(msg);

        if (messageData.type === 'file') {
          setUploadError(msg);
          setUploading(false);
        }
      }
    },
    [currentUser, roomId, handleSessionError, socketRef, uploading]
  );

  const handleEmojiToggle = useCallback(() => {
    setShowEmojiPicker((prev) => !prev);
  }, []);

  /**
   * 멘션 후보 필터링
   */
  const getFilteredParticipants = useCallback(
    (room) => {
      if (!room?.participants) return [];

      const filter = mentionFilter;
      if (!filter) return room.participants;

      return room.participants.filter((user) => {
        const name = user.name?.toLowerCase() || '';
        const email = user.email?.toLowerCase() || '';
        return name.includes(filter) || email.includes(filter);
      });
    },
    [mentionFilter]
  );

  /**
   * @뒤에 멘션 삽입
   */
  const insertMention = useCallback(
    (messageInputRef, user) => {
      if (!messageInputRef?.current || !user?.name) return;

      const input = messageInputRef.current;
      const cursorPosition = input.selectionStart ?? message.length;
      const textBeforeCursor = message.slice(0, cursorPosition);
      const atSymbolIndex = textBeforeCursor.lastIndexOf('@');

      if (atSymbolIndex === -1) return;

      const textBeforeAt = message.slice(0, atSymbolIndex);
      const newMessage =
        textBeforeAt + `@${user.name} ` + message.slice(cursorPosition);

      setMessage(newMessage);
      setShowMentionList(false);

      // 커서 위치 조정
      setTimeout(() => {
        const newPosition = atSymbolIndex + user.name.length + 2;
        input.focus();
        input.setSelectionRange(newPosition, newPosition);
      }, 0);
    },
    [message]
  );

  const removeFilePreview = useCallback(() => {
    setFilePreview(null);
    setUploadError(null);
    setUploadProgress(0);
  }, []);

  return {
    message,
    showEmojiPicker,
    showMentionList,
    mentionFilter,
    mentionIndex,
    filePreview,
    uploading,
    uploadProgress,
    uploadError,
    setMessage,
    setShowEmojiPicker,
    setShowMentionList,
    setMentionFilter,
    setMentionIndex,
    setFilePreview,
    handleMessageChange,
    handleMessageSubmit,
    handleEmojiToggle,
    handleLoadMore,
    getFilteredParticipants,
    insertMention,
    removeFilePreview,
  };
};

export default useMessageHandling;
