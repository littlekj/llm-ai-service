"""提示词工程模块，管理不同场景的提示模板"""
from typing import Dict, List, Optional, Any


class PromptTemplates:
    """提示词模板集合"""

    @staticmethod
    def get_document_qa_prompt() -> str:
        """获取文档问答提示"""
        return """你是一个专业的文档分析和AI助手，请遵循以下规则：

                1. 如果提供了文档内容：
                   - 仅基于提供的文档内容回答
                   - 引用具体的文档段落支持你的回答
                   - 如果文档中没有完整的答案，可以基于文档内容给出部分答案
                
                2. 如果没有提供文档内容：
                   - 基于你的知识提供专业的建议和答案
                   - 保持答案的可操作性和实用性
                   - 适当补充解释和示例
                
                3. 始终保持：
                   - 答案简洁准确
                   - 结构清晰
                   - 实用性强"""

    @staticmethod
    def get_code_review_prompt(language: str) -> str:
        """获取代码审查提示
        Args:
            language: 编程语言   
        Returns:
            str: 格式化的提示模板
        """
        return f"""你是一个{language}专家代码审查员，请从以下几个方面审查代码：
    
                1. 代码质量和最佳实践
                2. 性能优化机会
                3. 安全隐患
                4. 可维护性改进
                5. 具体的代码改进建议

                请用markdown格式输出，包含代码示例。"""

    @staticmethod
    def get_api_design_prompt() -> str:
        """获取API设计提示"""
        return """你是一个API设计专家，请：
    
                1. 遵循RESTful设计原则
                2. 考虑安全性和认证
                3. 提供详细的接口文档
                4. 包含请求/响应示例
                5. 说明错误处理机制"""

    @staticmethod
    def get_translation_prompt(source_lang: str, target_lang: str) -> str:
        """获取翻译提示"""
        return f"""你是一个专业的翻译专家，请将文本从{source_lang}翻译成{target_lang}：
    
                1. 保持原文的专业术语准确性
                2. 考虑目标语言的文化背景
                3. 维持原文的语气和风格
                4. 对专业术语提供注解"""

    @staticmethod
    def get_data_analysis_prompt() -> str:
        """获取数据分析提示"""
        return """你是一个数据分析专家，请：
    
                1. 提供关键数据洞察
                2. 指出数据中的模式和趋势
                3. 建议可能的分析方向
                4. 考虑数据质量问题
                5. 提供可视化建议"""

    @staticmethod
    def format_chat_context(
        template: str,
        context: Optional[str] = None,
        history: Optional[List[Dict[str, str]]] = None,
        **kwargs: Any
    ) -> List[Dict[str, str]]:
        """格式化聊天上下文
        Args:
            template: 基础提示模板
            context: 补充上下文信息
            history: 历史对话记录
            **kwargs: 其他模板变量 
        Returns:
            List[Dict[str, str]]: 格式化的消息列表
        """
        # 如果传入了kwargs且模板中包含占位符,则进行格式化
        if kwargs and '{' in template and '}' in template:
            try:
                messages = [
                    {"role": "system", "content": template.format(**kwargs)}]
            except KeyError as e:
                # 如果格式化失败,则使用原始模板
                messages = [{"role": "system", "content": template}]
        else:
            # 没有变量需要替换,直接使用原始模板
            messages = [{"role": "system", "content": template}]

        if context:
            messages.append({
                "role": "system",
                "content": f"补充信息：\n{context}"
            })

        if history:
            messages.extend(history)

        return messages
