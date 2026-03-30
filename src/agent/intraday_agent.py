#!/usr/bin/env python3
"""
Intraday Agent 核心模块
功能：
1. 调用 DeepSeek API 获取交易决策
2. 处理 API 请求/响应/重试
3. 输出原始 LLM 响应供 parser 解析

设计原则：
- 无 MCP 框架，直接 HTTP 调用
- 严格超时控制，避免阻塞盘中循环
- 支持模拟模式（记录但不执行）
"""
import os
import json
import logging
import time
from typing import Optional, Dict, Any, List
from datetime import datetime
import requests

from config import settings
from . import prompts

logger = logging.getLogger(__name__)

# DeepSeek API 配置
API_BASE = settings.DEEPSEEK_API_BASE
API_KEY = settings.DEEPSEEK_API_KEY
MODEL = settings.DEEPSEEK_MODEL

# 请求参数
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # 秒
REQUEST_TIMEOUT = 30  # 秒
TEMPERATURE = 0.1  # 低温度保证输出格式稳定
MAX_TOKENS = 500  # 限制输出长度，便于解析


class IntradayAgent:
    """盘中决策 Agent"""
    
    def __init__(self, api_key: str = None, model: str = None):
        self.api_key = api_key or API_KEY
        self.model = model or MODEL
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        })
        
        if not self.api_key:
            logger.warning("⚠️ DeepSeek API Key 未配置，将使用模拟模式")
    
    def _build_request_payload(self, system_prompt: str, user_message: str = "") -> Dict:
        """构建 API 请求体"""
        messages = [
            {"role": "system", "content": system_prompt}
        ]
        if user_message:
            messages.append({"role": "user", "content": user_message})
        
        return {
            "model": self.model,
            "messages": messages,
            "temperature": TEMPERATURE,
            "max_tokens": MAX_TOKENS,
            "stop": ["\n\n"]  # 可选：在双换行处停止，便于解析
        }
    
    def _call_api(self, payload: Dict, retry_count: int = 0) -> Optional[Dict]:
        """调用 DeepSeek API（带重试）"""
        try:
            response = self.session.post(
                f"{API_BASE}/chat/completions",
                json=payload,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            result = response.json()
            
            # 解析响应
            if "choices" in result and len(result["choices"]) > 0:
                content = result["choices"][0]["message"]["content"]
                return {
                    "content": content,
                    "usage": result.get("usage", {}),
                    "model": result.get("model", self.model)
                }
            else:
                logger.warning(f"⚠️ API 响应格式异常: {result}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"⏰ 请求超时，重试 {retry_count+1}/{MAX_RETRIES}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"🔌 连接错误，重试 {retry_count+1}/{MAX_RETRIES}")
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "N/A"
            logger.error(f"❌ HTTP {status} 错误: {e}")
            if status == 429:  # Rate limit
                logger.warning("🚦 触发限流，等待后重试")
            elif status >= 500:  # Server error
                pass  # 可重试
            else:
                return None  # 客户端错误不重试
        except Exception as e:
            logger.error(f"❌ 未知错误: {e}")
        
        # 重试逻辑
        if retry_count < MAX_RETRIES:
            time.sleep(RETRY_DELAY * (2 ** retry_count))  # 指数退避
            return self._call_api(payload, retry_count + 1)
        
        logger.error("❌ 所有重试失败")
        return None
    
    def decide(self, system_prompt: str, user_message: str = "", 
               simulate: bool = None) -> Optional[Dict[str, Any]]:
        """
        执行决策：调用 LLM 获取交易指令
        
        Args:
            system_prompt: 渲染后的 System Prompt
            user_message: 可选的用户补充消息
            simulate: 是否模拟模式（None 时读取配置）
        
        Returns:
            {
                "raw_output": str,      # LLM 原始输出
                "parsed": Optional[Dict], # 解析后的指令（由 parser 填充）
                "usage": Dict,          # Token 使用统计
                "timestamp": str,
                "simulate": bool
            }
        """
        if simulate is None:
            simulate = not settings.ENABLE_REAL_TRADING
        
        logger.info(f"🤖 Agent 决策开始 [模拟={simulate}]")
        start_time = time.time()
        
        # 1. 构建请求
        payload = self._build_request_payload(system_prompt, user_message)
        logger.debug(f"📤 请求: model={self.model}, tokens_limit={MAX_TOKENS}")
        
        # 2. 调用 API
        if not self.api_key or simulate:
            # 模拟模式：返回预设响应
            logger.info("🎭 模拟模式：返回预设响应")
            result = {
                "content": "# SIMULATED RESPONSE\n# No action needed at this time.\nNO_OP",
                "usage": {"prompt_tokens": 0, "completion_tokens": 10, "total_tokens": 10},
                "model": "simulated"
            }
        else:
            result = self._call_api(payload)
            if not result:
                logger.error("❌ API 调用失败，返回空决策")
                return {
                    "raw_output": None,
                    "parsed": None,
                    "usage": {},
                    "timestamp": datetime.now().isoformat(),
                    "simulate": simulate,
                    "error": "API call failed"
                }
        
        # 3. 记录日志
        elapsed = time.time() - start_time
        usage = result.get("usage", {})
        logger.info(
            f"✅ 决策完成 | 耗时: {elapsed:.2f}s | "
            f"Tokens: {usage.get('total_tokens', 'N/A')} | "
            f"输出长度: {len(result['content'])}"
        )
        
        # 4. 返回结构化结果
        return {
            "raw_output": result["content"].strip(),
            "parsed": None,  # 由 parser.py 后续填充
            "usage": usage,
            "timestamp": datetime.now().isoformat(),
            "simulate": simulate,
            "model": result.get("model", self.model)
        }
    
    def batch_decide(self, contexts: List[Dict[str, Any]], 
                    delay_between: float = 0.5) -> List[Dict[str, Any]]:
        """
        批量决策（用于多股票并行评估）
        
        Args:
            contexts: 多个股票的上下文列表
            delay_between: 请求间隔（避免限流）
        
        Returns:
            决策结果列表
        """
        results = []
        for i, ctx in enumerate(contexts):
            logger.info(f"🔄 批量决策 {i+1}/{len(contexts)}: {ctx.get('symbol', 'N/A')}")
            
            # 渲染单股票 Prompt（简化版，实际可能需要调整模板）
            system_prompt = prompts.build_prompt(ctx)
            result = self.decide(system_prompt)
            results.append(result)
            
            # 限流控制
            if i < len(contexts) - 1:
                time.sleep(delay_between)
        
        return results
    
    def close(self):
        """关闭会话"""
        if hasattr(self, 'session') and self.session:
            self.session.close()
            logger.info("🔌 Agent 会话已关闭")


def create_agent() -> IntradayAgent:
    """工厂函数：创建 Agent 实例"""
    return IntradayAgent(
        api_key=settings.DEEPSEEK_API_KEY,
        model=settings.DEEPSEEK_MODEL
    )