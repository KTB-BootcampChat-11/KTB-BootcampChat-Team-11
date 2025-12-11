import React, {
  useState,
  useEffect,
  useCallback,
  forwardRef,
  useMemo,
} from 'react';
import { Avatar } from '@vapor-ui/core';
import { generateColorFromEmail, getContrastTextColor } from '@/utils/colorUtils';

/**
 * CustomAvatar 컴포넌트
 *
 * @param {Object} props
 * @param {Object} props.user - 사용자 객체 (id, name, email, profileImage 필드)
 * @param {string} props.size - 아바타 크기 ('sm' | 'md' | 'lg' | 'xl')
 * @param {Function} props.onClick - 클릭 핸들러 (있으면 button으로 렌더링)
 * @param {string} props.src - 프로필 이미지 URL (user.profileImage 대신 직접 지정 가능)
 * @param {boolean} props.showImage - 이미지 표시 여부 (기본값: true)
 * @param {boolean} props.persistent - 실시간 프로필 업데이트 감지 여부 (기본값: false)
 * @param {boolean} props.showInitials - 이니셜 표시 여부 (기본값: true)
 * @param {string} props.className - 추가 CSS 클래스
 * @param {Object} props.style - 추가 인라인 스타일
 */

// 컴포넌트 밖으로 빼서 참조가 매번 바뀌지 않게
const buildImageUrl = (rawPath) => {
  if (!rawPath) return null;

  // 이미 풀 URL이면 그대로 사용
  if (rawPath.startsWith('http') || rawPath.startsWith('data:')) {
    return rawPath;
  }

  const base =
    process.env.NEXT_PUBLIC_S3_BASE_URL ||
    process.env.NEXT_PUBLIC_API_URL ||
    '';

  return `${base.replace(/\/$/, '')}/${rawPath.replace(/^\//, '')}`;
};

const CustomAvatar = forwardRef(
  (
    {
      user,
      size = 'md',
      onClick,
      src,
      showImage = true,
      persistent = false,
      showInitials = true,
      className = '',
      style = {},
      ...props
    },
    ref
  ) => {
    // persistent 모드에서만 의미 있는 상태
    const [currentImage, setCurrentImage] = useState('');
    const [imageError, setImageError] = useState(false);

    // 이메일 기반 배경색/텍스트 색상 생성
    const backgroundColor = generateColorFromEmail(user?.email);
    const color = getContrastTextColor(backgroundColor);

    // 🔹 localStorage에 저장된 유저 정보에서 프로필 경로 우선 사용
    const storedProfileImage = useMemo(() => {
      if (typeof window === 'undefined') return undefined;

      try {
        const stored = JSON.parse(localStorage.getItem('user') || '{}');
        if (!stored || !stored.id) return undefined;

        // 같은 유저일 때만 사용
        if (user?.id && stored.id === user.id) {
          return stored.profileImage || stored.profileImageUrl;
        }
      } catch {
        // 무시
      }
      return undefined;
    }, [user?.id]);

    // ✅ 최종 이미지 URL 계산 (렌더마다 재계산하지만 순수 함수라 OK)
    const finalImageUrl = useMemo(() => {
      if (!showImage) return undefined;

      // 1순위: props.src
      if (src) return buildImageUrl(src);

      // 2순위: localStorage에 저장된 URL
      if (storedProfileImage) return buildImageUrl(storedProfileImage);

      // 3순위: user.profileImage
      if (user?.profileImage) return buildImageUrl(user.profileImage);

      return undefined;
    }, [showImage, src, storedProfileImage, user?.profileImage]);

    // persistent 모드: finalImageUrl 기준으로 state 동기화
    useEffect(() => {
      if (!persistent) return;

      if (finalImageUrl && finalImageUrl !== currentImage) {
        setImageError(false);
        setCurrentImage(finalImageUrl);
      } else if (!finalImageUrl) {
        setCurrentImage('');
      }
    }, [persistent, finalImageUrl, currentImage]);

    // 프로필 업데이트 이벤트 리스너
    useEffect(() => {
      if (!persistent) return;

      const handleProfileUpdate = () => {
        try {
          const updatedUser = JSON.parse(localStorage.getItem('user') || '{}');
          if (user?.id && updatedUser.id === user.id) {
            const newImageUrl = buildImageUrl(
              updatedUser.profileImage || updatedUser.profileImageUrl
            );
            setImageError(false);
            setCurrentImage(newImageUrl || '');
          }
        } catch (error) {
          console.error('Profile update handling error:', error);
        }
      };

      window.addEventListener('userProfileUpdate', handleProfileUpdate);
      return () => {
        window.removeEventListener('userProfileUpdate', handleProfileUpdate);
      };
    }, [persistent, user?.id]);

    // 이미지 에러 핸들러
    const handleImageError = useCallback(
      (e) => {
        if (!persistent) return;

        e.preventDefault();
        setImageError(true);

        console.debug('Avatar image load failed:', {
          user: user?.name,
          email: user?.email,
          imageUrl: currentImage || finalImageUrl,
        });
      },
      [persistent, currentImage, finalImageUrl, user?.name, user?.email]
    );

    // 🔹 최종 src 결정
    const resolvedSrc = persistent
      ? currentImage && !imageError
        ? currentImage
        : undefined
      : finalImageUrl;

    // 사용자 이름 첫 글자
    const initial = showInitials
      ? user?.name?.charAt(0)?.toUpperCase() || '?'
      : '';

    // 클릭 가능한 경우 button으로 렌더링
    const renderProp = onClick ? <button onClick={onClick} /> : undefined;

    return (
      <Avatar.Root
        ref={ref}
        key={user?._id || user?.id}
        shape="circle"
        size={size}
        render={renderProp}
        src={resolvedSrc}
        className={className}
        style={{
          backgroundColor,
          color,
          cursor: onClick ? 'pointer' : 'default',
          ...style,
        }}
        {...props}
      >
        {resolvedSrc && (
          <Avatar.ImagePrimitive
            onError={persistent ? handleImageError : undefined}
            alt={`${user?.name}'s profile`}
          />
        )}
        <Avatar.FallbackPrimitive
          style={{ backgroundColor, color, fontWeight: '500' }}
        >
          {initial}
        </Avatar.FallbackPrimitive>
      </Avatar.Root>
    );
  }
);

CustomAvatar.displayName = 'CustomAvatar';

export default CustomAvatar;
