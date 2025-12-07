import logging
from typing import Optional, List, Dict, Any
from uuid import UUID


logger = logging.getLogger(__name__)

class TextSplitter:
    """
    文本分割器，用于将长文本拆分成固定大小的文本块，方便后续处理。
    """
    def __init__(
        self,
        chunk_size: int = 512,  # 每个文本块的最大字符数
        chunk_overlap: int = 50,  # 文本块之间的重叠字符数
        min_chunk_size: int = 100,  # 块的最小字符数
    ):
        if chunk_overlap >= chunk_size:
            raise ValueError("chunk_overlap must be smaller than chunk_size")
        
        if min_chunk_size >= chunk_size:
            raise ValueError("min_chunk_size must be smaller than chunk_size")
        
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size
        
        logger.info(
            f"TextSplitter initialized with chunk_size={chunk_size}, "
            f"chunk_overlap={chunk_overlap}, min_chunk_size={min_chunk_size}"
        )
        
        
    def split_text(
        self,
        text: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        分割文本为多个块，返回包含内容和元数据的列表。
        :param text: 待分割的文本
        :param metadata: 可选的元数据，附加到每个文本块
        :return: 文本块列表，每个块为字典，包含 'content' 和 'metadata'
        """
        chunks = []

        paragraphs = text.split('\n\n')
        
        current_chunk = ""
        chunk_index = 0
        
        for paragraph in paragraphs:
            if not paragraph.strip():
                continue  # 跳过空段落
            
            # 检查当前块加上新段落是否超过限制
            if len(current_chunk) + len(paragraph) > self.chunk_size:
                if current_chunk.strip():
                    chunks.append({
                        "content": current_chunk.strip(),
                        "metadata": {
                            **(metadata or {}),
                            "chunk_index": chunk_index,
                        }
                    })
                    
                    chunk_index += 1
                    
                # 开始新块，包含重叠部分
                overlap_part = current_chunk[-self.chunk_overlap:] if current_chunk else ""
                
                # 处理单个段落超过 chunk_size 的情况
                remaining_capacity = self.chunk_size - len(overlap_part)
                
                if len(paragraph) > remaining_capacity:
                    # 段落太长，需进一步递归拆分段落
                    sub_chunks = self._split_long_paragraph(
                        paragraph=paragraph,
                        overlap_part=overlap_part,
                        chunk_index=chunk_index,
                        metadata=metadata,
                    )
                    chunks.extend(sub_chunks[:-1])  # 添加除最后一个块外的所有子块
                    
                    # 更新当前块为最后一个子块的内容，并更新索引
                    if sub_chunks:
                        current_chunk = sub_chunks[-1]["content"]
                        chunk_index = sub_chunks[-1]["metadata"].get("chunk_index", chunk_index) + 1
                        
                else:
                    # 段落长度正常，直接追加
                    current_chunk = overlap_part + paragraph  
                    
            else:
                # 当前块未满，继续追加段落
                if current_chunk.strip():  
                    current_chunk += "\n\n" + paragraph
                else:
                    current_chunk = paragraph
                
        # 添加最后一个块
        if current_chunk.strip() and len(current_chunk.strip()) >= self.min_chunk_size:
            chunks.append({
                "content": current_chunk.strip(),
                "metadata": {
                    **(metadata or {}),
                    "chunk_index": chunk_index,
                }
            })
        # 如果最后一个块太小，合并到前一个块
        elif current_chunk.strip():
            if chunks:
                chunks[-1]["content"] += "\n\n" + current_chunk.strip()
                
        logger.info(f"Split text into {len(chunks)} chunks")
        
        return chunks
    
    def _split_long_paragraph(
        self,
        paragraph: str,
        overlap_part: str,
        chunk_index: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        递归拆分过长的段落为多个块。
        :param paragraph: 待拆分的段落
        :param overlap_text: 重叠文本
        :param chunk_index: 当前块索引
        :param metadata: 可选的元数据
        :return: 文本块列表
        """
        sub_chunks = []
        
        # 按句子拆分段落
        sentences = self._split_into_sentences(paragraph)
        
        current_sub_chunk = overlap_part
        
        for sentence in sentences:
            if len(current_sub_chunk) + len(sentence) > self.chunk_size:
                # 当前子块已满，保存并开始新块
                if current_sub_chunk.strip():
                    sub_chunks.append({
                        "content": current_sub_chunk.strip(),
                        "metadata": {
                            **(metadata or {}),
                            "chunk_index": chunk_index,
                        }
                    })
                    chunk_index += 1
                
                # 保留重叠部分，继续
                current_sub_chunk = current_sub_chunk[-self.chunk_overlap:] + sentence
            else:
                current_sub_chunk += sentence
        
        # 添加最后一个块        
        if current_sub_chunk.strip():
            sub_chunks.append({
                "content": current_sub_chunk.strip(),
                "metadata": {
                    **(metadata or {}),
                    "chunk_index": chunk_index,
                }
            })
                    
        return sub_chunks
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """
        简单的句子分割器，根据标点符号拆分文本为句子列表。
        :param text: 待分割的文本
        :return: 句子列表
        """
        import re
        
        # 使用正则表达式拆分句子
        sentences = re.split(r'([\.\!\?。！？])', text)
        
        # 重新组合标点符号和句子
        result = []
        for i in range(0, len(sentences) - 1, 2):
            if i + 1 < len(sentences):
                result.append(sentences[i] + sentences[i + 1])
            else:
                result.append(sentences[i])
                
        # 处理最后一个元素（如果没有标点符号结尾）
        if len(sentences) % 2 == 1 and sentences[-1].strip():
            result.append(sentences[-1])
            
        
        return [s.strip() for s in result if s.strip()]
