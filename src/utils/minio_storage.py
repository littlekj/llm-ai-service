from minio import Minio
from minio.commonconfig import REPLACE, Filter, CopySource, Tags
from minio.commonconfig import ENABLED, Tag
from minio.versioningconfig import VersioningConfig
from minio.lifecycleconfig import LifecycleConfig, Rule, Expiration
from minio.lifecycleconfig import NoncurrentVersionExpiration
from minio.deleteobjects import DeleteObject
from minio.error import S3Error, InvalidResponseError, ServerError
from minio.helpers import ObjectWriteResult
from typing import Optional, Dict, BinaryIO, Iterator, Any
from datetime import timedelta, datetime, timezone
from uuid import UUID
from urllib import parse
from base64 import b64encode, b64decode
import threading
import logging

from src.config.settings import settings


logger = logging.getLogger(__name__)


class MinioClient:
    def __init__(
        self, 
        endpoint_url: str = settings.MINIO_ENDPOINT, 
        access_key: str = settings.MINIO_ROOT_USER, 
        secret_key: str = settings.MINIO_ROOT_PASSWORD, 
        secure: bool = settings.MINIO_SECURE, 
        bucket_name: str = settings.MINIO_BUCKET_NAME
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
        
        # 桶名称
        self.bucket_name = bucket_name
        
        # 缓存已确认存在的桶，避免重复检查
        self._existing_buckets = set()
        
        # 桶锁，防止并发创建桶时的竞态条件
        self._bucket_lock = threading.Lock()
        
        # 调用确保桶存在并启用版本控制
        self._ensure_bucket_and_versioning()
        
        logger.info(
            "Minio client initialized",
            extra={
                "endpoint": endpoint_url,
                "bucket": bucket_name,
                "secure": secure
            }
        )

    def _ensure_bucket_and_versioning(self) -> bool:
        """
        确保桶存在并启用版本控制
        :param bucket_name: 桶名称
        :return: 是否成功创建桶
        """
        with self._bucket_lock:
            # 检查是否已缓存存在
            if self.bucket_name in self._existing_buckets:
                return True

            try:
                # 检查并创建桶
                if not self.client.bucket_exists(self.bucket_name):
                    self.client.make_bucket(self.bucket_name)
                    logger.info(f"Bucket '{self.bucket_name}' created successfully")
                
                # 检查并启用版本控制
                config = self.client.get_bucket_versioning(self.bucket_name)
                if config.status != ENABLED:
                    self.client.set_bucket_versioning(self.bucket_name, VersioningConfig(ENABLED))
                    logger.info(f"Versioning enabled for bucket '{self.bucket_name}")

                # 缓存结果
                self._existing_buckets.add(self.bucket_name)
                return True
            
            except S3Error as e:
                if e.code == "BucketAlreadyOwnedByYou":
                    # 防止并发创建桶时的竞态条件
                    self._existing_buckets.add(self.bucket_name)
                    return True
                logger.error(f"Failed to setup bucket '{self.bucket_name}': {e}", exc_info=True)
                return False
            except Exception as e:
                logger.error(f"Unexpected error setting up bucket '{self.bucket_name}': {e}", exc_info=True)
                return False
            
    def _encode_metadata(self, metadata: Optional[dict]) -> Optional[dict]:
        """
        对元数据进行编码，确保所有值均为 ASCII 字符
        """
        if not metadata:
            return None
        
        encoded_metadata = {}
        for key, value in metadata.items():
            str_value = str(value)
            try:
                # 尝试直接编码为 ASCII
                str_value.encode('ascii')
                encoded_metadata[key] = str_value
            except UnicodeEncodeError:
                # 包含非 ASCII 字符时，进行 base64 编码
                # 非常规字符串 -> UTF-8 字节序列 -> base64 字节序列 -> ASCII 字符串
                encoded_value = b64encode(str_value.encode('utf-8')).decode('ascii')
                encoded_metadata[f"{key}_b64"] = encoded_value
        
        return encoded_metadata
    
    def _decode_metadata(self, metadata: Optional[dict]) -> Optional[dict]:
        """
        解码元数据，将 base64 编码的值还原为原始字符串
        """
        if not metadata:
            return None
        
        decoded_metadata = {}
        for key, value in metadata.items():
            if key.endswith("_b64"):
                try:
                    original_key = key[:-4]  # 移除 "_b64" 后缀
                    decoded_value = b64decode(value).decode('utf-8')
                    decoded_metadata[original_key] = decoded_value
                except Exception as e:
                    logger.warning(f"Failed to decode base64 metadata for key {key}: {e}")
                    decoded_metadata[key] = value  # 保留原始编码值
            else:
                
                decoded_metadata[key] = value  # 保留原始编码值

        return decoded_metadata
    
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
        tags["deleted_at"] = deleted_at.isoformat()
        tags["soft_deleted"] = "true"
        
        return tags
    
    def _get_object_tags(self, object_name: str) -> Optional[Dict[str, str]]:
        """
        获取对象的标签
        """
        try:
            tags = self.client.get_object_tags(self.bucket_name, object_name)
            if tags:
                raw_tags = dict(tags)  # 注意：tags 是 Tag 类，._tags 是 dict
                return self._decode_metadata(raw_tags) 
            return None
        except S3Error as e:
            if e.code != "NoSuchKey":
                logger.error(f"Get tags failed: {str(e)}")
            return None 

    def upload_file(
        self,
        object_name: str,
        file_path: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict] = None,
    ) -> ObjectWriteResult:
        """
        上传文件到 Minio
        :param object_name: 对象名称（在桶中的路径）
        :param file_path: 文件路径
        :param content_type: 内容类型
        :param metadata: 自定义元数据（键值对需为 ASCII）
        :return: 是否上传成功
        """      
        try:
            result = self.client.fput_object(
                bucket_name=self.bucket_name, 
                object_name=object_name, 
                file_path=file_path, 
                content_type=content_type, 
                metadata=self._encode_metadata(metadata),
            )
            logger.info(
                "Object upload completed",
                extra={
                    "operation": "upload",
                    "bucket": self.bucket_name,
                    "object_key": object_name,
                    "version_id": result.version_id,
                    "etag": result.etag,
                    "content_type": content_type,
                }
            )
            return result  # 返回结果对象，包含 etag\version_id 等信息
            
        except S3Error as exc:
            # S3 协议错误（如权限不足、Bucket 不存在等）
            if exc.code == "NoSuchBucket":
                logger.critical(f"Bucket does not exist: {self.bucket_name}")
            elif exc.code == "AccessDenied":
                logger.critical(f"Access denied uploading to {self.bucket_name}/{object_name}")
            else:
                logger.error(f"S3 error udring upload: {exc.code} - {exc.message}")
            raise  # 重新抛出异常，以便调用者处理
        
        except (ServerError, InvalidResponseError) as exc:
            # 服务器错误或无效响应，可能是暂时的，稍后重试
            logger.warning(f"Transient error during upload: {type(exc).__name__}: {exc}")
            raise
        except Exception as exc:
            # 其他非预期异常
            logging.error(f"Unexpected error during upload: {exc}", exc_info=True)
            raise
    
    # TODO: 实现上传文件对象的方法
    def upload_fileobj(
        self,
        object_name: str,
        data: BinaryIO,
        length: int,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict] = None,
    ) -> bool:
        """
        上传文件对象（如 BytesIO）
        """
        try:
            result = self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=data,
                length=length,
                metadata=metadata,
            )
            logger.info(f"File object uploaded: {object_name}")
            return True
        except S3Error as e:
            logger.error(f"S3 uploaded failed: {e.code} - {e.message}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during file object upload: {e}", exc_info=True)
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
        
    def _latest_version(self, object_name: str):
        """
        获取最新版本
        """
        try:
            # 获取对象的所有版本
            objects = self.client.list_objects(
                self.bucket_name, 
                prefix=object_name,
                include_version=True
            )

            versions = list(objects)
            logger.info("List object versions fetched", extra={"object_prefix": object_name, "versions_count": len(versions)})
            
            # 获取最新版本
            def _last_modified(v):
                _last_modified = getattr(v, "last_modified", None)
                return _last_modified if _last_modified is not None else datetime.min.replace(tzinfo=timezone.utc)
            latest_version = max(versions, key=_last_modified) if versions else None

            return latest_version if latest_version else None
        
        except S3Error as e: 
            if e.code == "NoSuchKey":
                logger.warning(f"Object {object_name} does not exist")
                return None
            logger.error(f"Failed to list object latest versions for {object_name}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing object latest version for {object_name}: {e}", exc_info=True)
            raise
        
    def _latest_delete_marker_version(self, object_name: str):
        """
        获取最新删除标记版本
        """
        try:
            # 获取对象的所有版本
            objects = self.client.list_objects(
                self.bucket_name, 
                prefix=object_name,
                include_version=True
            )

            versions = list(objects)
            logger.info("List object versions fetched", extra={"object_prefix": object_name, "versions_count": len(versions)})
            
            def _last_modified(v):
                _last_modified = getattr(v, "last_modified", None)
                return _last_modified if _last_modified is not None else datetime.min.replace(tzinfo=timezone.utc)
            
            delete_marker_versions = [v for v in versions if getattr(v, "is_delete_marker", False)]
            latest_delete_marker_version = max(delete_marker_versions, key=_last_modified) if delete_marker_versions else None
            if latest_delete_marker_version is None:
                return None
            return latest_delete_marker_version
        
        except S3Error as e: 
            if e.code == "NoSuchKey":
                logger.warning(f"Object {object_name} does not exist")
                return None
            logger.error(f"Failed to list object latest versions for {object_name}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing object latest delete marker version for {object_name}: {e}", exc_info=True)
            raise
    
    def soft_delete_document(self, object_name: str):
        """
        使用版本控制实现软删除：添加 Delete Marker
        """
        try:
            # 添加 Delete Marker（即软删除） 
            # 注意：这里不会删除数据，只会添加 Delete Marker         
            self.client.remove_object(self.bucket_name, object_name)  
            logger.info(f"Deleted marker added successfully for {object_name}")
            
            try:
                # 当对象有删除标记时，stat_object 默认会返回最新版本（即删除标记），而删除标记被视为对象不存在。
                # obj_stat = self.client.stat_object(self.bucket_name, object_name)
                
                latest_version = None
                latest_delete_marker_version = None
                
                # 获取最新版本
                latest_version = self._latest_version(object_name)
                latest_delete_marker_version = self._latest_delete_marker_version(object_name)

                # 验证最新版本是否为删除标记版本
                if not latest_delete_marker_version or not latest_version:
                    raise ValueError(f"Invalid version information for {object_name}")
                if not (latest_delete_marker_version.last_modified == latest_version.last_modified) and \
                    not (latest_delete_marker_version.version_id == latest_version.version_id):
                    raise ValueError(f"Delete marker version is not the latest version for {object_name}")
                # 获取删除标记版本 ID
                if latest_delete_marker_version.version_id:
                    delete_marker_version_id = latest_delete_marker_version.version_id
                else:
                    # 如果 delete marker 版本 ID 为空，则使用当前时间作为标识
                    delete_marker_version_id = f"delete_marker_{datetime.now(timezone.utc).timestamp()}"
                 
            except S3Error as e:
                if e.code == "NoSuchKey":
                    raise ValueError(f"Object {object_name} does not exist or has not delete marker")
                raise
                    
            # # 设置对象的标签
            # deleted_at = datetime.now(timezone.utc)
            # tags = self._build_object_tagging(doc_id, user_id, deleted_at)
            # self.client.set_object_tags(self.bucket_name, object_name, tags)
            
            logger.info(f"Soft deleted via delete marker: s3://{self.bucket_name}/{object_name}")
                
            return {
                "status": "soft_deleted",
                "object_name": object_name,
                "delete_marker_version_id": delete_marker_version_id,
                "bucket_name": self.bucket_name,
                "deleted_at": datetime.now(timezone.utc).isoformat(),
            }
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                return {
                    "status": "already_deleted",
                    "object_name": object_name,
                    "bucket_name": self.bucket_name,
                }
            logger.error(f"Failed to soft delete object {object_name}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during soft delete: {e}", exc_info=True)
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
    
    def restore_document(self, object_name: str, version_id: str) -> Dict:
        """
        恢复被软删除的对象（删除 Delete Marker）
        """
        latest_version = None
        latest_delete_marker_version = None
        try:    
            # 获取对象的所有版本
            versions = self.client.list_objects(
                self.bucket_name,
                prefix=object_name,
                include_version=True,
            )
            versions = list(versions)
            def _last_modified(v):
                _last_modified = getattr(v, "last_modified", None)
                return _last_modified if _last_modified is not None else datetime.min.replace(tzinfo=timezone.utc)
            
            # 获取最新版本和删除标记版本
            latest_version = max(versions, key=_last_modified) if versions else None
            delete_marker_versions = [v for v in versions if getattr(v, "is_delete_marker", False)]
            latest_delete_marker_version = max(delete_marker_versions, key=_last_modified) if delete_marker_versions else None
            
            # 验证最新版本是否为删除标记版本
            if not latest_delete_marker_version or not latest_version:
                raise ValueError(f"Invalid version information for {object_name}")
            if not (latest_delete_marker_version.last_modified == latest_version.last_modified) and \
                not (latest_delete_marker_version.version_id == latest_version.version_id):
                raise ValueError(f"Delete marker version is not the latest version for {object_name}")
            # 获取删除标记版本 ID
            if not latest_delete_marker_version.version_id:
                raise ValueError(f"Delete marker version ID is empty for {object_name}")
            delete_marker_version_id = latest_delete_marker_version.version_id
            if delete_marker_version_id != version_id:
                raise ValueError(f"Delete marker version mismatch for {object_name}")

            # 删除 Delete Marker，恢复对象
            self.client.remove_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                version_id=delete_marker_version_id,
            )
            logger.info(f"Delete marker removed for {object_name}")
        except (S3Error, InvalidResponseError, ServerError) as e:
            logger.error(f"Failed to remove delete marker for {object_name}: {e}")
            raise
        
        # 查找最新版本信息
        restored_version_id = None
        try:
            current_version = self._latest_version(object_name)

            if not current_version:
                raise ValueError(f"Object {object_name} does not exist after delete marker removal")
            if current_version.is_delete_marker:
                raise ValueError(f"After delete marker removal, new delete marker exists for {object_name}")
            else:
                restored_version_id = current_version.version_id
                logger.info(f"Restored version for {object_name} with version {restored_version_id}")
                    
            return {
                "status": "restored", 
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "last_version": restored_version_id,
            }
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.error(f"Object {object_name} does not exist after delete marker removal")
                raise
            logger.error(f"S3 error while checking restored version: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during restored version check: {e}", exc_info=True)
            raise
        
    def permanent_delete_version(self, object_name: str, version_id: Optional[str] = None):
        """
        永久删除某个版本对象
        """
        try:
            # 删除特定版本的对象
            self.client.remove_object(self.bucket_name, object_name, version_id=version_id)
            logger.info(f"Permanently deleted: {object_name}, version: {version_id or 'latest'}")
        except S3Error as e:
            if e.code == "NoSuchKey":
                logger.info(f"Object {object_name} does not exist or already deleted")
            else:
                logger.warning(f"Failed to permanently delete {object_name}: {e}")
        except Exception as e:
            logger.error(f"Permanent delete failed for {object_name}: {e}", exc_info=True)
            raise
        
    def permanent_delete_document(self, object_name: str):
        """
        永久删除对象的所有版本
        """
        try:
            versions = self.client.list_objects(
                self.bucket_name,
                prefix=object_name,
                include_version=True,
            )
            
            for version in versions:
                is_delete_marker = version.is_delete_marker
                if is_delete_marker:
                    logger.info(f"Delete marker version {version.version_id} found for object {object_name}")
                else:
                    logger.info(f"Non-delete marker version {version.version_id} found for object {object_name}")
                self.client.remove_object(
                    self.bucket_name,
                    object_name,
                    version_id=version.version_id,
                )
                logger.info(f"Deleted version {version.version_id} of object {object_name}")

        except S3Error as e:
            logger.error(f"Failed to permanently delete all versions for object {object_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during permanent delete all version for object {object_name}: {e}", exc_info=True)
            raise
        
    def setup_lifecycle_policy(self, days: int = 30):
        """
        设置生命周期策略：自动清理非当前版本
        """
        config = LifecycleConfig(
            rules=[
                Rule(
                    rule_filter=Filter(prefix="", tag=Tag("soft_deleted", "true")),
                    rule_id="auto-cleanup-deleted-docs",
                    status="Enabled",
                    expiration=Expiration(days=days),
                    noncurrent_version_expiration=NoncurrentVersionExpiration(
                        noncurrent_days=days  
                    ),
                )
            ]
        )
        
        self.client.set_bucket_lifecycle(self.bucket_name, config)
        logger.info(f"Lifecycle policy applied to delete non-current versions after {days} days")

    def list_objects_in_trash(self, prefix: str = "trash/"):
        """
        列出 trash 目录下的所有对象（生成器）
        """
        try:
            return self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
        except S3Error as e:
            logger.error(f"Failed to list objects in trash: {e}")
            return []
        
    def list_objects(self, prefix: Optional[str] = None, recursive: bool = True) -> Iterator:
        """
        列出桶中所有对象（支持分页）
        """
        try:
            return self.client.list_objects(
                self.bucket_name, 
                prefix=prefix, 
                recursive=recursive,
                include_version=True,
            )
        except S3Error as e:
            logger.error(f"S3Error listing objects: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing objects: {e}", exc_info=True)
            raise
        
    def get_object(self, storage_key: str) -> Any:
        try:
            return self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=storage_key
            )
        except S3Error as e:
            logger.error(f"Failed to get object {storage_key}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error getting object {storage_key}: {e}", exc_info=True)
            raise

    def get_presigned_url(
        self, 
        object_name: str, 
        expires: timedelta = timedelta(days=1),
        response_headers: Optional[dict] = None
    ) -> str:
        """
        生成预签名下载链接
        :param bucket_name: 桶名称
        :param object_name: 对象名称（在桶中的路径）
        :param expires: 链接有效期（默认为1天）
        :return: 预签名链接，如果生成失败则返回 None
        """
        try:
            url = self.client.presigned_get_object(
                bucket_name=self.bucket_name, 
                object_name=object_name, 
                expires=expires,
                response_headers=response_headers or {}
            )
            logger.info(f"Presigned URL generated for {object_name}, expires in {expires}")
            return url
        except S3Error as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            raise 
        except Exception as e:
            logger.error(f"Unexpected error generating presigned URL: {e}", exc_info=True)
            raise
