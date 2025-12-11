import axios from "axios";
import axiosInstance from "./axios";

class FileService {
  constructor() {
    this.baseUrl = process.env.NEXT_PUBLIC_API_URL;
    this.uploadLimit = 50 * 1024 * 1024; // 50MB
    this.retryAttempts = 3;
    this.retryDelay = 1000;

    // 업로드 관리 (AbortController 기반)
    this.activeUploads = new Map();

    this.allowedTypes = {
      image: {
        extensions: [".jpg", ".jpeg", ".png", ".gif", ".webp"],
        mimeTypes: ["image/jpeg", "image/png", "image/gif", "image/webp"],
        maxSize: 10 * 1024 * 1024,
        name: "이미지",
      },
      document: {
        extensions: [".pdf"],
        mimeTypes: ["application/pdf"],
        maxSize: 20 * 1024 * 1024,
        name: "PDF 문서",
      },
    };
  }

  /** 공통 에러 메시지 */
  getErrorMessageByStatus(status, defaultMsg = "알 수 없는 오류가 발생했습니다.") {
    switch (status) {
      case 400:
        return "잘못된 요청입니다.";
      case 401:
        return "인증이 필요합니다.";
      case 403:
        return "파일에 접근할 권한이 없습니다.";
      case 404:
        return "파일을 찾을 수 없습니다.";
      case 413:
        return "파일이 너무 큽니다.";
      case 415:
        return "지원하지 않는 파일 형식입니다.";
      case 500:
        return "서버 오류가 발생했습니다.";
      case 503:
        return "서비스를 일시적으로 사용할 수 없습니다.";
      default:
        return defaultMsg;
    }
  }

  /** 파일 확장자 추출 */
  getFileExtension(filename) {
    if (!filename) return "";
    const parts = filename.split(".");
    return parts.length > 1 ? `.${parts.pop().toLowerCase()}` : "";
  }

  /** 파일 크기 포맷 */
  formatFileSize(bytes) {
    if (!bytes || bytes === 0) return "0 B";
    const units = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${units[i]}`;
  }

  /** 파일 검증 */
  async validateFile(file) {
    if (!file) {
      return { success: false, message: "파일이 선택되지 않았습니다." };
    }

    if (file.size > this.uploadLimit) {
      return {
        success: false,
        message: `파일 크기는 ${this.formatFileSize(this.uploadLimit)} 이하여야 합니다.`,
      };
    }

    let matchedConfig = null;

    for (const config of Object.values(this.allowedTypes)) {
      if (config.mimeTypes.includes(file.type)) {
        matchedConfig = config;
        break;
      }
    }

    if (!matchedConfig) {
      return { success: false, message: "지원하지 않는 파일 형식입니다." };
    }

    if (file.size > matchedConfig.maxSize) {
      return {
        success: false,
        message: `${matchedConfig.name} 파일은 ${this.formatFileSize(
          matchedConfig.maxSize
        )} 까지만 업로드할 수 있습니다.`,
      };
    }

    const ext = this.getFileExtension(file.name);
    if (!matchedConfig.extensions.includes(ext)) {
      return { success: false, message: "파일 확장자가 올바르지 않습니다." };
    }

    return { success: true };
  }

  /** 업로드 전송용 고유 ID 생성 */
  createUploadId(file) {
    return `${file.name}-${file.size}-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2)}`;
  }

  /** presigned URL 가져오기 */
  async fetchPresignedResource(filename, type = "download") {
    const encoded = encodeURIComponent(filename);
    const endpoint = type === "view" ? "view" : "download";

    const url = this.baseUrl
      ? `${this.baseUrl}/api/files/${endpoint}/${encoded}`
      : `/api/files/${endpoint}/${encoded}`;

    const response = await axiosInstance.get(url, { withCredentials: true });

    if (!response?.data?.success) {
      throw new Error(response?.data?.message || "presigned URL 생성 실패");
    }
    return response.data;
  }

  /** presigned 헤더 정리 */
  sanitizePresignedHeaders(headers = {}) {
    const sanitized = {};
    Object.entries(headers).forEach(([key, value]) => {
      if (!key) return;
      if (key.toLowerCase() === "host") return;
      sanitized[key] = value;
    });
    return sanitized;
  }

  /** 이미지면 리사이징 + 압축 시도 */
  async resizeImageIfNeeded(file, options = {}) {
    const {
      maxWidth = 800,
      maxHeight = 800,
      quality = 0.5,
      minResizeThreshold = 100 * 1024, // 100KB 이상만 리사이즈
    } = options;

    // 이미지 아니면 패스
    if (!file.type.startsWith("image/")) return file;

    // 작은 파일은 손 안 댐
    if (file.size < minResizeThreshold) return file;

    // createImageBitmap 없는 오래된 브라우저면 그냥 기존 로직/원본 사용
    if (typeof createImageBitmap !== "function") {
      return file;
    }

    try {
      // 1) 브라우저에게 디코딩 맡기기
      const bitmap = await createImageBitmap(file);

      const { width, height } = bitmap;
      const widthRatio = maxWidth / width;
      const heightRatio = maxHeight / height;
      const ratio = Math.min(widthRatio, heightRatio, 1);

      // 리사이즈 필요 없음
      if (ratio === 1) {
        bitmap.close?.();
        return file;
      }

      const targetWidth = Math.round(width * ratio);
      const targetHeight = Math.round(height * ratio);

      // 2) 캔버스에 브라우저가 알아서 스케일링하게 시킴
      const canvas = document.createElement("canvas");
      canvas.width = targetWidth;
      canvas.height = targetHeight;
      const ctx = canvas.getContext("2d");

      // PNG → JPEG 될 때 투명 배경 대비용
      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, targetWidth, targetHeight);

      ctx.drawImage(bitmap, 0, 0, targetWidth, targetHeight);
      bitmap.close?.();

      const outputType = file.type || "image/jpeg";

      // 3) 브라우저에게 인코딩까지 맡기기
      const blob = await new Promise((resolve, reject) => {
        canvas.toBlob(
          (b) => {
            if (!b) {
              reject(new Error("이미지 리사이즈에 실패했습니다."));
              return;
            }
            resolve(b);
          },
          outputType,
          quality
        );
      });

      const resizedFile = new File([blob], file.name, {
        type: outputType,
        lastModified: Date.now(),
      });

      return resizedFile;
    } catch (e) {
      // 리사이즈 실패 시 원본으로 업로드
      console.error("resizeImageIfNeeded error:", e);
      return file;
    }
  }


  /** 파일 업로드 */
  async uploadFile(file, onProgress) {
    const valid = await this.validateFile(file);
    if (!valid.success) return valid;

    const uploadId = this.createUploadId(file);

    // 기본은 원본 파일, 이미지면 리사이징 시도
    let uploadFile = file;
    try {
      if (file.type.startsWith("image/")) {
        uploadFile = await this.resizeImageIfNeeded(file);
        console.log(uploadFile.size);
      }
    } catch (e) {
      // 리사이즈 실패해도 업로드 자체는 진행 (원본 사용)
      uploadFile = file;
    }

    try {
      const url = this.baseUrl
        ? `${this.baseUrl}/api/files/upload`
        : "/api/files/upload";

      const metadata = {
        originalFilename: file.name, // 원래 파일 이름은 그대로 저장
        contentType: uploadFile.type || file.type || "application/octet-stream",
        size: uploadFile.size,
      };

      // presigned URL 요청
      const presignedRes = await axiosInstance.post(url, metadata, {
        withCredentials: true,
      });

      if (!presignedRes.data?.success) {
        return {
          success: false,
          message: presignedRes.data?.message || "업로드 URL을 가져오지 못했습니다.",
        };
      }

      const presigned = presignedRes.data.presignedUrl;

      // AbortController 등록
      const controller = new AbortController();
      this.activeUploads.set(uploadId, controller);

      const headers = this.sanitizePresignedHeaders({
        ...(presigned.headers || {}),
        "Content-Type":
          uploadFile.type || file.type || "application/octet-stream",
      });

      await axios({
        method: presigned.method || "PUT",
        url: presigned.url,
        data: uploadFile,
        signal: controller.signal,
        headers,
        onUploadProgress: (e) => {
          if (!onProgress) return;

          const total = e.total || uploadFile.size || file.size;
          const percent = Math.round((e.loaded / total) * 100);

          // 업로드 이벤트 과도 호출 방지 (100ms throttle)
          const now = Date.now();
          if (!this._lastEmit || now - this._lastEmit > 100) {
            this._lastEmit = now;
            onProgress(percent);
          }
        },
      });

      this.activeUploads.delete(uploadId);

      return { success: true, uploadId, data: presignedRes.data };
    } catch (err) {
      this.activeUploads.delete(uploadId);

      if (err.name === "CanceledError" || err.code === "ERR_CANCELED") {
        return { success: false, message: "업로드가 취소되었습니다." };
      }

      if (err.response) {
        return {
          success: false,
          message:
            err.response.data?.message ||
            this.getErrorMessageByStatus(err.response.status, "업로드 실패"),
        };
      }

      return {
        success: false,
        message: err.message || "알 수 없는 오류 발생",
      };
    }
  }

  /** 업로드 취소 */
  cancelUpload(uploadId) {
    const controller = this.activeUploads.get(uploadId);
    if (!controller) {
      return { success: false, message: "취소할 업로드가 없습니다." };
    }
    controller.abort();
    this.activeUploads.delete(uploadId);
    return { success: true, message: "업로드가 취소되었습니다." };
  }

  /** 전체 업로드 취소 */
  cancelAllUploads() {
    let count = 0;
    for (const [id, controller] of this.activeUploads) {
      controller.abort();
      this.activeUploads.delete(id);
      count++;
    }
    return { success: true, message: `${count}개의 업로드가 취소되었습니다.` };
  }

  /** 파일 다운로드 */
  async downloadFile(filename, originalname) {
    let blobUrl = null;

    try {
      const presigned = await this.fetchPresignedResource(filename, "download");
      const p = presigned.presignedUrl;

      const headers = this.sanitizePresignedHeaders(p.headers);

      const res = await axios({
        method: p.method || "GET",
        url: p.url,
        responseType: "blob",
        headers,
      });

      const blob = new Blob([res.data], {
        type: res.headers["content-type"] || "application/octet-stream",
      });

      const contentDisposition = res.headers["content-disposition"];
      let finalName = originalname;

      if (contentDisposition) {
        const match = contentDisposition.match(
          /filename\*=UTF-8''([^;]+)|filename="([^"]+)"|filename=([^;]+)/
        );
        if (match) {
          finalName = decodeURIComponent(match[1] || match[2] || match[3]);
        }
      }

      blobUrl = URL.createObjectURL(blob);
      const link = document.createElement("a");

      link.href = blobUrl;
      link.download = finalName;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      return { success: true };
    } catch (err) {
      if (err.response) {
        return {
          success: false,
          message:
            err.response.data?.message ||
            this.getErrorMessageByStatus(
              err.response.status,
              "파일 다운로드 실패"
            ),
        };
      }

      return { success: false, message: err.message || "알 수 없는 오류 발생" };
    } finally {
      if (blobUrl) setTimeout(() => URL.revokeObjectURL(blobUrl), 200);
    }
  }

  /** 파일 미리보기 URL */
  async getPreviewUrl(file) {
    if (!file?.filename) return "";
    const res = await this.fetchPresignedResource(file.filename, "view");
    return res?.presignedUrl?.url || "";
  }
}

export default new FileService();
