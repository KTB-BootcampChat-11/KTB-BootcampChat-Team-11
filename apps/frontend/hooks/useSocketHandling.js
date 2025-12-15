import { useState, useRef, useCallback, useEffect } from 'react';
import socketService from '../services/socket';
import { Toast } from '../components/Toast';

export const useSocketHandling = (router, maxRetries = 5) => {
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState(null);
  const [retryCount, setRetryCount] = useState(0);
  const [isReconnecting, setIsReconnecting] = useState(false);

  const socketRef = useRef(null);
  const retryTimeoutRef = useRef(null);
  const connectionTimeoutRef = useRef(null);
  const retryCountRef = useRef(0); // 로직용 카운터 (state와 분리)

  const cleanup = useCallback(() => {
    if (retryTimeoutRef.current) {
      clearTimeout(retryTimeoutRef.current);
      retryTimeoutRef.current = null;
    }
    if (connectionTimeoutRef.current) {
      clearTimeout(connectionTimeoutRef.current);
      connectionTimeoutRef.current = null;
    }
  }, []);

  const getRetryDelay = useCallback((retryAttempt) => {
    return Math.min(1000 * Math.pow(2, retryAttempt), 10000); // 최대 10초
  }, []);

  /**
   * 재시도 처리
   * - attempt를 인자로 받아서 클로저 꼬임 방지
   */
  const handleConnectionError = useCallback(
    async (errorObj, handleSessionError, attemptOverride) => {
      setConnected(false);
      setIsReconnecting(true);

      const attempt =
        typeof attemptOverride === 'number' ? attemptOverride : retryCountRef.current;

      try {
        const msg = errorObj?.message || '';

        // 세션 관련 에러는 바로 세션 에러 처리로 위임
        if (
          msg.includes('세션') ||
          msg.includes('인증') ||
          msg.includes('토큰')
        ) {
          await handleSessionError?.();
          return;
        }

        if (attempt < maxRetries) {
          const nextAttempt = attempt + 1;
          retryCountRef.current = nextAttempt;
          setRetryCount(nextAttempt);

          const retryDelay = getRetryDelay(attempt);

          cleanup();

          retryTimeoutRef.current = setTimeout(async () => {
            try {
              if (!socketRef.current) {
                // 아직 소켓 생성이 안 되어 있으면 재시도 중단
                return;
              }

              await socketRef.current.connect();
              setConnected(true);
              setIsReconnecting(false);
              retryCountRef.current = 0;
              setRetryCount(0);
              setError(null);

              const roomId = router?.query?.room;
              if (roomId) {
                socketRef.current.emit('joinRoom', roomId);
              }
            } catch (retryError) {
              // 다음 시도 넘겨서 재귀 호출
              handleConnectionError(retryError, handleSessionError, nextAttempt);
            }
          }, retryDelay);
        } else {
          setIsReconnecting(false);
          Toast.error('채팅 서버와 연결할 수 없습니다. 페이지를 새로고침해주세요.');
        }
      } catch (e) {
        setIsReconnecting(false);
      }
    },
    [cleanup, getRetryDelay, maxRetries, router?.query?.room]
  );

  /**
   * 명시적인 재연결 시도
   */
  const handleReconnect = useCallback(
    async (currentUser, handleSessionError) => {
      if (isReconnecting) return;

      // currentUser 없으면 조용히 종료 (online 이벤트에서 호출될 수 있음)
      if (!currentUser?.token || !currentUser?.sessionId) {
        return;
      }

      try {
        setError(null);
        retryCountRef.current = 0;
        setRetryCount(0);
        setIsReconnecting(true);

        cleanup();

        if (socketRef.current) {
          socketRef.current.disconnect();
          setConnected(false);
        }

        const socket = await socketService.connect({
          auth: {
            token: currentUser.token,
            sessionId: currentUser.sessionId,
          },
          transports: ['websocket', 'polling'],
          reconnection: true,
          reconnectionAttempts: maxRetries,
          reconnectionDelay: getRetryDelay(0),
          reconnectionDelayMax: 10000,
          timeout: 20000,
        });

        socketRef.current = socket;

        // 연결 타임아웃
        connectionTimeoutRef.current = setTimeout(() => {
          if (!socket.connected) {
            handleConnectionError(
              new Error('Connection timeout'),
              handleSessionError,
              retryCountRef.current
            );
          }
        }, 20000);

        // 연결 성공 핸들러
        const onConnect = () => {
          setConnected(true);
          setIsReconnecting(false);
          retryCountRef.current = 0;
          setRetryCount(0);
          setError(null);
          cleanup();

          const roomId = router?.query?.room;
          if (roomId) {
            socket.emit('joinRoom', roomId);
          }
        };

        // 연결 에러 핸들러
        const onConnectError = (err) => {
          handleConnectionError(err, handleSessionError, retryCountRef.current);
        };

        // disconnect 핸들러
        const onDisconnect = (reason) => {
          setConnected(false);

          if (
            reason === 'io server disconnect' ||
            reason === 'io client disconnect'
          ) {
            // 의도적 종료는 재시도 안 함
            return;
          }

          handleConnectionError(
            new Error(`Disconnected: ${reason}`),
            handleSessionError,
            retryCountRef.current
          );
        };

        socket.on('connect', onConnect);
        socket.on('connect_error', onConnectError);
        socket.on('disconnect', onDisconnect);
      } catch (errorObj) {
        setConnected(false);
        setIsReconnecting(false);

        const msg = errorObj?.message || '';

        if (
          msg.includes('세션') ||
          msg.includes('인증') ||
          msg.includes('토큰')
        ) {
          await handleSessionError?.();
          return;
        }

        Toast.error('재연결에 실패했습니다.');
      }
    },
    [
      isReconnecting,
      cleanup,
      getRetryDelay,
      maxRetries,
      router?.query?.room,
      handleConnectionError,
    ]
  );

  // 언마운트 시 타이머 정리
  useEffect(() => {
    return cleanup;
  }, [cleanup]);

  /**
   * 네트워크 상태 모니터링
   * - online 시: currentUser를 전달해서 외부에서 호출하는 걸 권장
   *   (여기서는 currentUser를 모를 수 있어서 그냥 no-op이 될 수 있음)
   */
  useEffect(() => {
    const handleOnline = () => {
      // 여기서는 currentUser를 모르는 상태라, 외부에서 넘겨준 경우에만 의미 있게 동작
      // 사용자가 직접 handleReconnect(currentUser, handleSessionError)를 호출하는 게 더 안정적
    };

    const handleOffline = () => {
      setConnected(false);
    };

    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);

    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);

  return {
    connected,
    error,
    socketRef,
    isReconnecting,
    setConnected,
    setError,
    handleConnectionError,
    handleReconnect,
    cleanup,
  };
};

export default useSocketHandling;
