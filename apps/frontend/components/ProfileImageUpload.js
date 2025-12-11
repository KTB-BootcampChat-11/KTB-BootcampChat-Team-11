import React, { useState, useRef, useEffect } from 'react';
import axios from 'axios';
import { CameraIcon, CloseOutlineIcon } from '@vapor-ui/icons';
import { Button, Text, Callout, VStack, HStack } from '@vapor-ui/core';
import { useAuth } from '@/contexts/AuthContext';
import CustomAvatar from '@/components/CustomAvatar';
import { Toast } from '@/components/Toast';
import axiosInstance from '@/services/axios';

const ProfileImageUpload = ({ currentImage, onImageChange }) => {
  const { user } = useAuth();
  const [previewUrl, setPreviewUrl] = useState(null);
  const [error, setError] = useState('');
  const [uploading, setUploading] = useState(false);
  const fileInputRef = useRef(null);

  const sanitizePresignedHeaders = (headers = {}) => {
    const sanitized = {};
    Object.entries(headers || {}).forEach(([key, value]) => {
      if (!key) return;
      if (key.toLowerCase() === 'host') return;
      sanitized[key] = value;
    });
    return sanitized;
  };

  // 프로필 이미지 URL 생성
  const getProfileImageUrl = (imagePath) => {
    if (!imagePath) return null;
    return imagePath.startsWith('http') ?
      imagePath :
      `${process.env.NEXT_PUBLIC_API_URL}${imagePath}`;
  };

  // 컴포넌트 마운트 시 이미지 설정
  useEffect(() => {
    const imageUrl = getProfileImageUrl(currentImage);
    setPreviewUrl(imageUrl);
  }, [currentImage]);

  const handleFileSelect = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    let objectUrl = null;

    try {
      // 이미지 파일 검증
      if (!file.type.startsWith('image/')) {
        throw new Error('이미지 파일만 업로드할 수 있습니다.');
      }

      // 파일 크기 제한 (5MB)
      if (file.size > 5 * 1024 * 1024) {
        throw new Error('파일 크기는 5MB를 초과할 수 없습니다.');
      }

      setUploading(true);
      setError('');

      // 파일 미리보기 생성
      objectUrl = URL.createObjectURL(file);
      setPreviewUrl(objectUrl);

      const metadata = {
        originalFilename: file.name,
        contentType: file.type || 'application/octet-stream',
        size: file.size
      };

      const response = await axiosInstance.post(
        '/api/users/profile-image',
        metadata,
        { withCredentials: true }
      );

      const { data } = response || {};
      if (!data?.success || !data?.presignedUrl?.url) {
        throw new Error(data?.message || '프로필 이미지 업로드 URL을 생성하지 못했습니다.');
      }

      const uploadHeaders = sanitizePresignedHeaders({
        ...(data.presignedUrl.headers || {}),
        'Content-Type': file.type || 'application/octet-stream'
      });

      await axios({
        method: data.presignedUrl.method || 'PUT',
        url: data.presignedUrl.url,
        data: file,
        headers: uploadHeaders
      });

      if (objectUrl?.startsWith('blob:')) {
        URL.revokeObjectURL(objectUrl);
        objectUrl = null;
      }
      const finalImageUrl = getProfileImageUrl(data.imageKey);
      setPreviewUrl(finalImageUrl);
      
      // 로컬 스토리지의 사용자 정보 업데이트
      const updatedUser = {
        ...user,
        profileImage: data.imageKey
      };
      localStorage.setItem('user', JSON.stringify(updatedUser));

      // 부모 컴포넌트에 변경 알림
      onImageChange(data.imageKey);

      Toast.success('프로필 이미지가 변경되었습니다.');

      // 전역 이벤트 발생
      window.dispatchEvent(new Event('userProfileUpdate'));

    } catch (error) {
      console.error('Image upload error:', error);
      setError(error.message);
      setPreviewUrl(getProfileImageUrl(currentImage));

      // 기존 objectUrl 정리
      if (objectUrl && objectUrl.startsWith('blob:')) {
        URL.revokeObjectURL(objectUrl);
        objectUrl = null;
      }
    } finally {
      setUploading(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const handleRemoveImage = async () => {
    try {
      setUploading(true);
      setError('');

      await axiosInstance.delete('/api/users/profile-image', {
        withCredentials: true
      });

      // 로컬 스토리지의 사용자 정보 업데이트
      const updatedUser = {
        ...user,
        profileImage: ''
      };
      localStorage.setItem('user', JSON.stringify(updatedUser));

      // 기존 objectUrl 정리
      if (previewUrl && previewUrl.startsWith('blob:')) {
        URL.revokeObjectURL(previewUrl);
      }

      setPreviewUrl(null);
      onImageChange('');

      // 전역 이벤트 발생
      window.dispatchEvent(new Event('userProfileUpdate'));

    } catch (error) {
      console.error('Image removal error:', error);
      setError(error.message);
    } finally {
      setUploading(false);
    }
  };

  // 컴포넌트 언마운트 시 cleanup
  useEffect(() => {
    return () => {
      if (previewUrl && previewUrl.startsWith('blob:')) {
        URL.revokeObjectURL(previewUrl);
      }
    };
  }, [previewUrl]);

  return (
    <VStack gap="$300" alignItems="center">
      <CustomAvatar
        user={user}
        size="xl"
        persistent={true}
        showInitials={true}
        src={previewUrl || getProfileImageUrl(currentImage)}
        data-testid="profile-image-avatar"
      />

      <HStack gap="$200" justifyContent="center">
        <Button
          type="button"
          onClick={() => fileInputRef.current?.click()}
          disabled={uploading}
          data-testid="profile-image-upload-button"
        >
          <CameraIcon />
          이미지 변경
        </Button>

        {previewUrl && (
          <Button
            type="button"
            variant="fill"
            colorPalette="danger"
            onClick={handleRemoveImage}
            disabled={uploading}
            data-testid="profile-image-delete-button"
          >
            <CloseOutlineIcon />
            이미지 삭제
          </Button>
        )}
      </HStack>

      <input
        ref={fileInputRef}
        type="file"
        className="hidden"
        accept="image/*"
        onChange={handleFileSelect}
        data-testid="profile-image-file-input"
      />

      {error && (
        <Callout color="danger">
          <HStack gap="$200" alignItems="center">
            <Text>{error}</Text>
          </HStack>
        </Callout>
      )}

      {uploading && (
        <Text typography="body3" color="$hint-100">
          이미지 업로드 중...
        </Text>
      )}
    </VStack>
  );
};

export default ProfileImageUpload;
