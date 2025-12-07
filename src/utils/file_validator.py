import sys
import re
import os
import logging
from pathlib import Path
from typing import Dict, Union, BinaryIO

from src.core.exceptions import ValidationError


logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {".pdf", ".docx", ".txt", ".md", ".pptx", ".xlsx"}

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

# 获取当前操作系统
system = sys.platform.lower()

if system == "win32":
    # 匹配规则的字符串注意使用方括号
    invalid_chars = r'[\/:*?"<>|]'  # Windows 系统中不允许的文件名字符
    reversed_names = {
        'con', 'prn', 'aux', 'nul', 'com1', 'com2', 'com3', 'com4', 'com5', 'com6', 'com7', 'com8', 'com9',
        'lpt1', 'lpt2', 'lpt3', 'lpt4', 'lpt5', 'lpt6', 'lpt7', 'lpt8', 'lpt9'
    }
else:
    invalid_chars = r'[\/]'
    reversed_names = set()


def sanitize_filename(filename: str) -> str:
    """
    标准化文件名，去除非法字符，统一编码，并避免 Windows 系统的保留文件名。
    """
    # 编码并忽略无法转换为UTF-8的字符
    encoded_filename = filename.encode('utf-8', errors='ignore')

    # 再次解码回来，确保文件名没有丢失字符或出现非法字符
    sanitized_filename = encoded_filename.decode('utf-8', errors='ignore')
    
    # 替换非法字符为下划线
    cleaned = re.sub(invalid_chars, '_', sanitized_filename)
    
    # 替换非ASCII字符为下划线
    cleaned = re.sub(r'\x00-\x7F', '_', cleaned)
    
    # 将多个下划线替换为一个下划线
    cleaned = re.sub(r'_+', '_', cleaned)

    # 去掉文件名首尾的空格、下划线和点
    cleaned = cleaned.strip(' _.')
    
    # 防止文件名为保留名称
    base_name, ext = os.path.splitext(cleaned)
    if base_name.lower() in reversed_names:
        base_name = f"{base_name}_file"
    cleaned = base_name + ext
    
    # 如果文件名为空，使用默认名称
    if not cleaned:
        cleaned = "unnamed"
       
    # 限制文件名长度（比如不超过255个字符，避免文件系统限制）
    cleaned = cleaned[:255]

    return cleaned
    
    
def sanitize_metadata(metadata: Dict[str, str]) -> Dict[str, str]:
    """
    标准化元数据，将所有元数据值中的非法字符替换为下划线
    """
    sanitized_metadata = {}
    for key, value in metadata.items():
        sanitized_key = sanitize_filename(key)
        sanitized_value = sanitize_filename(value)
        sanitized_metadata[sanitized_key] = sanitized_value
    return sanitized_metadata


async def validate_file_extension(filename: str) -> Union[str, None]:
    """
    校验文件扩展名，返回小写扩展名（含点）
    :param filename: 文件名
    :return: 文件扩展名，如果文件扩展名无效则返回None
    """
    # print("filename: ", filename)
    ext = Path(filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        logger.error(f"File type not allowed: {ext}")
        raise ValidationError(
            message=f"File type not allowed: {ext}",
            details={
                "filename": filename,
                "extension": ext,
                "allowed_extensions": list(ALLOWED_EXTENSIONS),
            }
        )
    return ext


def validate_file_size(file, max_size: int = MAX_FILE_SIZE) -> int:
    """
    校验文件大小并返回实际字节长度（边读边校验，避免全读入内存）
    :param file: file-like object
    :param max_size: 最大文件大小（字节）
    :return: 实际文件大小（字节）
    """
    
    file.seek(0, 2)  # 移动到文件末尾（标准值：0=开头，1=当前位置，2=末尾）
    file_size = file.tell()  # 获取文件大小
    file.seek(0)  # 重置文件指针到开头

    if file_size == 0:
        raise ValidationError(
            message="File size cannot be empty",
            details={"filename": file.name}
        )
        
    if file_size > max_size:
        raise ValidationError(
            message=f"File size exceeds limit ({max_size}) bytes",
            details={
                "filename": file.name,
                "file_size": file_size,
                "max_size": max_size
            }
        )
        
    return file_size


async def validate_file_size_async(file_obj: BinaryIO, max_size: int = MAX_FILE_SIZE) -> int:
    """
    校验文件大小并返回实际字节长度（边读边校验，避免全读入内存）
    :param file: file-like object
    :param max_size: 最大文件大小（字节）
    :return: 实际文件大小（字节）
    """
    total_size = 0
    original_pos = file_obj.tell() if hasattr(file_obj, 'tell') else 0  # 获取当前文件指针位置
    
    try:
        while True:
            chunk = file_obj.read(1024 * 8)  # 读取8KB的块
            if not chunk:
                break
            total_size += len(chunk)
            if total_size > max_size:
                raise ValidationError(
                    message=f"File size exceeds limit ({max_size}) bytes",
                    details={
                        "current_size": total_size,
                        "max_size": max_size
                    }
                )
        if total_size == 0:
            raise ValidationError(
                message="File size cannot be empty",
            )
        return total_size
    
    except IOError as e:
        raise ValidationError(
            message="Failed to read file",
            details={"error": str(e)}
        )
    finally:
        # 尝试重置文件指针位置
        if hasattr(file_obj, 'seek'):
            try:
                file_obj.seek(original_pos)
            except (IOError, OSError) as e:
                logger.warning(f"Failed to reset file pointer: {e}")     
