#!/usr/bin/env python3
"""
Prompt 模板管理模块（更新版：包含风控参数）
功能：
1. 加载 prompt_template.txt
2. 渲染占位符：{EST_time}, {daily_stock_pool}, {intraday_indicator_*_series}, {positions}, {max_daily_trades} 等
3. 输出最终 System Prompt 供 Agent 调用
"""
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

from config import settings

logger = logging.getLogger(__name__)

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent.parent
PROMPT_TEMPLATE_PATH = PROJECT_ROOT / "config" / "prompt_template.txt"


def load_template(template_path: Optional[str] = None) -> str:
    """加载原始 Prompt 模板文件"""
    path = Path(template_path) if template_path else PROMPT_TEMPLATE_PATH
    
    if not path.exists():
        logger.error(f"❌ Prompt 模板不存在：{path}")
        raise FileNotFoundError(f"Prompt template not found: {path}")
    
    with open(path, 'r', encoding='utf-8') as f:
        template = f.read()
    
    logger.info(f"✅ 加载 Prompt 模板：{path}")
    return template


def format_stock_pool_entry(symbol: str, momentum: Dict[str, bool], 
                           reference_level: str, notes: str = "") -> str:
    """
    格式化单只股票的动量评估为 Prompt 字符串
    
    输出格式（严格匹配 System Prompt 要求）：
    "{symbol}":{"5-min level strong momentum: True/False; 
                1-hour level strong momentum: True/False;
                1-day level strong momentum: True/False;
                1-week level strong momentum: True/False;
                Most suitable reference level: 5-min/1-hour/1-day/1-week"}
    """
    return f'''"{symbol}":{{"5-min level strong momentum: {str(momentum.get("5-min", False))}; 
                1-hour level strong momentum: {str(momentum.get("1-hour", False))};
                1-day level strong momentum: {str(momentum.get("1-day", False))};
                1-week level strong momentum: {str(momentum.get("1-week", False))};
                Most suitable reference level: {reference_level}"}}'''


def format_series_data(series: List[Dict], max_bars: int = None, 
                       fields: List[str] = None) -> str:
    """
    格式化时间序列数据为 Prompt 可读字符串
    
    Args:
        series: 数据列表，每项包含 datetime 和 indicators
        max_bars: 最多保留的条数（控制 Token）
        fields: 需要展示的字段列表，默认使用关键指标
    """
    if not series:
        return "N/A"
    
    if fields is None:
        fields = ["close", "vwap", "bbi", "bbiboll_upper", "bbiboll_lower", "bbiboll_ratio"]
    
    # 限制条数
    if max_bars and len(series) > max_bars:
        series = series[-max_bars:]
    
    lines = []
    for item in series:
        dt = item.get("datetime", "N/A")
        indicators = item.get("indicators", {})
        
        # 构建单行：时间 | key=value 对
        parts = [f"{dt}"]
        for field in fields:
            value = indicators.get(field, "N/A")
            # 格式化数值，避免过长
            if isinstance(value, (int, float)):
                value = f"{value:.4f}"
            parts.append(f"{field}:{value}")
        
        lines.append(" | ".join(parts))
    
    return "\n".join(lines)


def format_positions(positions: Dict[str, Any]) -> str:
    """
    格式化持仓信息为 Prompt 字符串
    
    输入格式示例：
    {
        "AAPL": {"shares": 100, "avg_cost": 150.5, "current_price": 155.2},
        "CASH": {"available": 50000.0}
    }
    
    输出格式：
    AAPL: 100 shares @ $150.5 (now $155.2)
    CASH: $50000.0 available
    """
    if not positions:
        return "No positions"
    
    lines = []
    for symbol, info in positions.items():
        if symbol == "CASH":
            available = info.get("available", 0)
            lines.append(f"CASH: ${available:,.2f} available")
        else:
            shares = info.get("shares", 0)
            avg_cost = info.get("avg_cost", 0)
            current = info.get("current_price", 0)
            pnl = (current - avg_cost) * shares if shares > 0 else 0
            pnl_pct = (current/avg_cost - 1) * 100 if avg_cost > 0 else 0
            
            lines.append(
                f"{symbol}: {shares} shares @ ${avg_cost:.2f} "
                f"(now ${current:.2f}, P/L ${pnl:+.2f} {pnl_pct:+.1f}%)"
            )
    
    return "\n".join(lines)


def render_prompt(template: str, context: Dict[str, Any]) -> str:
    """
    渲染 Prompt 模板，填充所有占位符
    
    支持的占位符：
    - {EST_time}: 当前美东时间
    - {daily_stock_pool}: 资产池动量评估（格式化字符串）
    - {intraday_indicator_5min_series}: 5 分钟序列数据
    - {intraday_indicator_1hour_series}: 小时序列数据
    - {intraday_indicator_1day_series}: 日线序列数据
    - {total_value}: 账户总值
    - {return}: 总收益率
    - {positions}: 持仓信息
    - {max_daily_trades}: 每日最大交易次数
    - {trades_today}: 今日已执行交易数
    - {trades_remaining}: 剩余可交易次数
    - {max_position_pct}: 单股票最大仓位比例
    - {min_cash_reserve}: 最小现金保留比例
    - {available_cash}: 可用现金
    - {trading_mode}: SIMULATED/LIVE
    """
    # 1. 风控参数占位符
    risk_placeholders = {
        "max_daily_trades": str(settings.MAX_DAILY_TRADES),
        "trades_today": str(context.get("trades_today", 0)),
        "trades_remaining": str(max(0, settings.MAX_DAILY_TRADES - context.get("trades_today", 0))),
        "max_position_pct": str(int(settings.MAX_POSITION_PER_STOCK * 100)),
        "min_cash_reserve": str(int(settings.MIN_CASH_RESERVE * 100)),
        "available_cash": f"${context.get('available_cash', 0):,.2f}",
        "trading_mode": "LIVE" if settings.ENABLE_REAL_TRADING else "SIMULATED",
    }
    
    # 2. 基础占位符替换
    placeholders = {
        "EST_time": context.get("est_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S EST")),
        "total_value": f"${context.get('total_value', 0):,.2f}",
        "return": f"{context.get('return', 0):+.2f}%",
        "positions": format_positions(context.get("positions", {})),
        "daily_stock_pool": context.get("prompt_ready", {}).get("daily_stock_pool_str", ""),
    }
    
    # 合并所有占位符
    all_placeholders = {**placeholders, **risk_placeholders}
    
    # 简单替换（单层）
    for key, value in all_placeholders.items():
        template = template.replace(f"{{{key}}}", str(value))
    
    # 3. 多股票数据块替换（复杂逻辑）
    stock_data = context.get("stock_data", {})
    prompt_ready = context.get("prompt_ready", {})
    
    if stock_data:
        asset_blocks = []
        for symbol, data in stock_data.items():
            # 构建单只股票的数据块
            block = f"""### ALL {symbol} DATA:
            **Intraday series (by minute, oldest → latest):**
            {prompt_ready.get(f"{symbol}_5min_series", "N/A")}

            **Longer‑term context (1‑hour timeframe):**
            {prompt_ready.get(f"{symbol}_hourly_series", "N/A")}

            **Longer‑term context (1‑day timeframe):**
            {prompt_ready.get(f"{symbol}_daily_series", "N/A")}
            """
            asset_blocks.append(block)
        
        # 替换模板中的重复结构标记
        if "{ALL_ASSETS_DATA}" in template:
            template = template.replace("{ALL_ASSETS_DATA}", "\n\n".join(asset_blocks))
        else:
            # 降级：直接追加到模板末尾（需要模板设计支持）
            logger.warning("⚠️ 模板中未找到 {ALL_ASSETS_DATA} 占位符，使用追加模式")
            template += "\n\n" + "\n\n".join(asset_blocks)
    
    return template


def build_prompt(context: Dict[str, Any], template_path: Optional[str] = None) -> str:
    """
    完整流程：加载模板 + 渲染上下文 → 最终 Prompt
    
    Args:
        context: 来自 build_daily_pool.py 的上下文数据
                 需要包含：est_time, positions, total_value, return, 
                          trades_today, available_cash, stock_data, prompt_ready
        template_path: 可选的自定义模板路径
    
    Returns:
        渲染后的完整 System Prompt 字符串
    """
    try:
        template = load_template(template_path)
        final_prompt = render_prompt(template, context)
        logger.info(f"✅ Prompt 渲染完成，长度：{len(final_prompt)} chars")
        return final_prompt
    except Exception as e:
        logger.error(f"❌ Prompt 构建失败：{e}")
        raise


def get_prompt_token_estimate(prompt: str) -> int:
    """
    估算 Prompt Token 数量（简化版）
    实际使用中可调用 tokenizer 精确计算
    """
    # 英文平均 1 token ≈ 4 字符
    return len(prompt) // 4