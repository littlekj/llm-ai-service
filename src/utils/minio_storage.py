from minio import Minio
from minio.commonconfig import REPLACE, CopySource, Tags
from minio.error import S3Error
from typing import Optional, Dict
from datetime import timedelta, datetime
from uuid import UUID
from urllib import parse
import logging


logger = logging.getLogger(__name__)


class MinioClient:
    def __init__(
        self, 
        endpoint_url: str, 
        access_key: str, 
        secret_key: str, 
        secure: bool, 
        bucket_name: str
    ):
        """
        初始化 Minio 客户端
        :param endpoint_url: Minio 服务器地址
        :param access_key: 访问密钥
        :param secret_key: 秘密密钥
        :param secure: 是否使用 HTTPS（生产环境应为 True）
        """
        self.client = Minio(
            endpoint=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.bucket_name = bucket_name
        
        # 缓存已确认存在的桶，避免重复检查
        self._existing_buckets = set()

    def _ensure_bucket_exists(self) -> bool:
        """
        确保桶存在，如果不存在则创建
        :param bucket_name: 桶名称
        :return: 是否成功创建桶
        """
        # 检查是否已缓存存在
        if self.bucket_name in self._existing_buckets:
            return True

        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Bucket '{self.bucket_name}' created successfully")
            self._existing_buckets.add(self.bucket_name)
            return True
        except S3Error as e:
            if e.code == "BucketAlreadyOwnedByYou":
                # 防止并发创建桶时的竞态条件
                self._existing_buckets.add(self.bucket_name)
                return True
            logger.error(f"Failed to create bucket '{self.bucket_name}': {e}")
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error while creating bucket '{self.bucket_name}': {e}", 
                exc_info=True
            )
            return False

    def upload_fileobj(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict] = None,
        auto_create_bucket: bool = True,
    ) -> bool:
        """
        上传文件对象到 Minio
        :param bucket_name: 桶名称
        :param object_name: 对象名称（在桶中的路径）
        :param file_path: 文件路径
        :param content_type: 内容类型
        :param metadata: 自定义元数据（键值对需为 ASCII）
        :param auto_create_bucket: 是否自动创建桶（默认为 True）
        :return: 是否上传成功
        """
        if auto_create_bucket:
            if not self._ensure_bucket_exists():
                logger.error(f"Bucket '{bucket_name}' does not exist and cannot be created")
                return False
                
        try:
            self.client.fput_object(
                bucket_name=self.bucket_name, 
                object_name=object_name, 
                file_path=file_path, 
                content_type=content_type, 
                metadata=metadata
            )
            return True
        except S3Error as e:
            logging.error(
                f"S3 uploaded failed: {e.code}, message: {e.message}, \
                resource: {e._resource}, request_id: {e._request_id}"
            )
            return False
        except Exception as e:
            logging.error(f"Unexpected error during file upload: {e}", exc_info=True)
            return False
        
    def _generate_trash_key(self, original_key: str) -> str:
        """
        生成软删除后的对象名称（在桶中的路径）
        """
        now = datetime.now()
        date_prefix = now.strftime("%Y%m%d")
        # URL encode key 防止特殊字符
        # encoded_key = parse.quote(original_key.strip("/"))
        base_trash = f"trash/{date_prefix}/{original_key}"
        
        return base_trash
    
    def _build_object_tagging(
        self, 
        doc_id: UUID, 
        user_id: int, 
        deleted_at: datetime
    ):
        """构建 tagging 元数据"""
        tags = Tags()
        tags["doc_id"] = str(doc_id)
        tags["user_id"] = str(user_id)
        tags["deleted_at"] = str(deleted_at)
        tags["soft_deleted"] = "true"
        
        return tags
    
    def soft_delete(
        self, 
        original_key: str,
        doc_id: UUID,
        user_id: int,
        deleted_at: datetime    
    ) -> dict:
        """
        软删除：将对象移动到 trash 目录，并保留原对象
        """
        if not original_key:
            raise ValueError("Original key cannot be empty")
        
        try:
            # 检查原对象是否存在
            self.client.stat_object(self.bucket_name, original_key)
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.warning(f"Object '{original_key}' does not exist")
                return {"status": "not_found", "moved_to": None}
            else:
                logger.error(f"Stat object failed: {str(e)}")
                raise
            
        new_key = self._generate_trash_key(original_key)
        
        try:
            source = CopySource(self.bucket_name, original_key)
            tags = self._build_object_tagging(doc_id, user_id, deleted_at)
            
            # 复制对象到 trash
            result = self.client.copy_object(
                bucket_name=self.bucket_name,
                object_name=new_key,
                source=source,
                tags=tags,
                tagging_directive="REPLACE"  # 强制使用新标签
            )
            
            # 删除原对象
            self.client.remove_object(self.bucket_name, original_key)
            
            return {
                "status": "success", 
                "moved_to": new_key,
                "etag": result.etag,
                "version_id": result.version_id
            }
        except Exception as e:
            logger.error(f"Soft delete failed for {original_key}: {str(e)}", exc_info=True)
            raise
    
    def restore(self, trash_key: str, original_key: str) -> bool:
        """
        恢复软删除的对象
        """
        try:
            copy_source = CopySource(self.bucket_name, trash_key)
            self.client.copy_object(
                bucket_name=self.bucket_name,
                object_name=original_key,
                source=copy_source
            )
            logger.info(f"Restored object from {trash_key} to {original_key}")
            return True
        except S3Error as e:
            logger.error(f"Failed to restore object from {trash_key} to {original_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during object restore: {e}", exc_info=True)
            return False
    
    def permanent_delete(self, trash_key: str):
        """永久删除（用于清理任务）"""
        try:
            self.client.remove_object(self.bucket_name, trash_key)
            logger.info(f"Permanently deleted: {trash_key}")
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.warning(f"Already deleted: {trash_key}")
            else:
                raise
        
    def get_object_tagging(self, key: str) -> Optional[Dict[str, str]]:
        """获取对象标签"""
        try:
            tags = self.client.get_object_tags(self.bucket_name, key)
            if tags:
                return dict(tags)  # 注意：tags 是 Tag 类，._tags 是 dict
            return None
        except S3Error as e:
            if e.code == "NoSuchKey":
                return None
            logger.error(f"Get tagging failed for {key}: {str(e)}")
            return None 

    def list_objects_in_trash(self, prefix: str = "trash/"):
        """
        列出 trash 目录下的所有对象（生成器）
        """
        try:
            return self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
        except S3Error as e:
            logger.error(f"Failed to list objects in trash: {e}")
            return []

    def get_presigned_url(
        self, 
        bucket_name: str, 
        object_name: str, 
        expires: timedelta = timedelta(days=1)
    ) -> Optional[str]:
        """
        生成预签名下载链接
        :param bucket_name: 桶名称
        :param object_name: 对象名称（在桶中的路径）
        :param expires: 链接有效期（默认为1天）
        :return: 预签名链接，如果生成失败则返回 None
        """
        try:
            url = self.client.presigned_get_object(
                bucket_name=bucket_name, 
                object_name=object_name, 
                expires=expires
            )
            return url
        except S3Error as e:
            logging.error(f"Failed to generate presigned URL: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error generating presigned URL: {e}", exc_info=True)
            return None
