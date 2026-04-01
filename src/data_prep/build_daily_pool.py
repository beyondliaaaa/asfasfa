#!/usr/bin/env python3
"""
盘前资产池构建模块
功能：
1. 读取 stock_pool_template.txt 解析资产池配置
2. 查询四层数据库获取每只股票的多粒度最新数据
3. 计算/验证动量评估（可选自动化逻辑）
4. 输出格式化的上下文数据供 Prompt 注入

运行时机：盘前 9:00 AM EST
输出：cache/context_{date}.json 或内存对象
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from shared.intraday_db import get_session, IntradayBar
from shared.ibkr_price_adapter import IBKRPriceAdapter

logger = logging.getLogger(__name__)


def parse_bool_field(value: str) -> bool:
    """解析模板中的布尔字段（支持 True/False/1/0/yes/no）"""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('true', '1', 'yes', 't', 'y')


def parse_stock_pool_template(template_path: str) -> List[Dict[str, Any]]:
    """
    解析 stock_pool_template.txt
    
    返回格式：
    [
        {
            "symbol": "AAPL",
            "momentum": {
                "5-min": True,
                "1-hour": False,
                "1-day": True,
                "1-week": False
            },
            "reference_level": "5-min",
            "notes": "earnings catalyst"
        },
        ...
    ]
    """
    pool = []
    
    if not os.path.exists(template_path):
        logger.error(f"模板文件不存在: {template_path}")
        return pool
    
    with open(template_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            # 跳过空行和注释
            if not line or line.startswith('#'):
                continue
            
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 6:
                logger.warning(f"行 {line_num} 格式错误，跳过: {line}")
                continue
            
            symbol = parts[0].upper()
            try:
                momentum = {
                    "5-min": parse_bool_field(parts[1]),
                    "1-hour": parse_bool_field(parts[2]),
                    "1-day": parse_bool_field(parts[3]),
                    "1-week": parse_bool_field(parts[4])
                }
                reference_level = parts[5].strip()
                notes = parts[6].strip() if len(parts) > 6 else ""
                
                # 验证 reference_level
                valid_levels = ["5-min", "1-hour", "1-day", "1-week"]
                if reference_level not in valid_levels:
                    logger.warning(f"{symbol}: 无效的 reference_level '{reference_level}'，使用默认 1-day")
                    reference_level = "1-day"
                
                pool.append({
                    "symbol": symbol,
                    "momentum": momentum,
                    "reference_level": reference_level,
                    "notes": notes
                })
                logger.info(f"✅ 解析: {symbol} | ref={reference_level} | momentum={sum(momentum.values())}/4")
                
            except (IndexError, ValueError) as e:
                logger.error(f"行 {line_num} 解析失败: {e} | 原始: {line}")
                continue
    
    logger.info(f"📋 资产池加载完成: {len(pool)} 只股票")
    return pool


def query_multi_granularity_data(symbol: str, adapter: IBKRPriceAdapter) -> Dict[str, Any]:
    """查询单只股票的多粒度最新数据"""
    result = {
        "symbol": symbol,
        "5min_series": [],
        "hourly_series": [],
        "daily_series": [],
        "latest": {}
    }
    
    # ── 1. 查询 intraday DB 获取 5 分钟数据 ──
    session = get_session()  # intraday_db 的 session
    try:
        from shared.intraday_db import IntradayBar
        
        bars_5min = session.query(IntradayBar).filter(
            IntradayBar.symbol == symbol
        ).order_by(IntradayBar.datetime.desc()).limit(settings.MAX_BARS_5MIN).all()
        
        if bars_5min:
            result["5min_series"] = [bar.to_dict() for bar in reversed(bars_5min)]
            result["latest"]["5min"] = bars_5min[0].to_dict()
            logger.debug(f"📊 {symbol}: 加载 {len(bars_5min)} 条 5 分钟数据")
    except Exception as e:
        logger.warning(f"⚠️ {symbol} 5 分钟数据查询失败：{e}")
    finally:
        session.close()
    
    # ── 2. 查询 historical DB 获取小时/日线数据 ──
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    
    hist_engine = create_engine(f'sqlite:///{settings.HISTORICAL_DB_PATH}')
    hist_session = sessionmaker(bind=hist_engine)()
    
    try:
        # 🔧 查询 HourlyBar 表（1 小时线）
        hourly_query = text("""
            SELECT symbol, datetime, open, high, low, close, volume,
                   vwap, bbi, bbiboll_upper, bbiboll_lower, bbiboll_ratio
            FROM hourly_bars
            WHERE symbol = :symbol
            ORDER BY datetime DESC
            LIMIT :limit
        """)
        hourly_result = hist_session.execute(hourly_query, {
            "symbol": symbol,
            "limit": settings.MAX_BARS_HOURLY
        }).fetchall()
        
        if hourly_result:
            # 转换为与 IntradayBar.to_dict() 兼容的格式
            for row in reversed(hourly_result):
                result["hourly_series"].append({
                    "symbol": row[0],
                    "datetime": str(row[1]) if row[1] else None,
                    "indicators": {
                        "close": str(row[5]) if row[5] else "N/A",
                        "vwap": str(row[7]) if row[7] else "N/A",
                        "bbi": str(row[8]) if row[8] else "N/A",
                        "bbiboll_upper": str(row[9]) if row[9] else "N/A",
                        "bbiboll_lower": str(row[10]) if row[10] else "N/A",
                        "bbiboll_ratio": str(row[11]) if row[11] else "N/A"
                    }
                })
            logger.debug(f"📊 {symbol}: 加载 {len(hourly_result)} 条小时数据")
        
        # 🔧 查询 DailyBar 表（日线）
        daily_query = text("""
            SELECT symbol, date, open, high, low, close, volume,
                   vwap, bbi, bbiboll_upper, bbiboll_lower, bbiboll_ratio
            FROM daily_bars
            WHERE symbol = :symbol
            ORDER BY date DESC
            LIMIT :limit
        """)
        daily_result = hist_session.execute(daily_query, {
            "symbol": symbol,
            "limit": settings.MAX_BARS_DAILY
        }).fetchall()
        
        if daily_result:
            for row in reversed(daily_result):
                result["daily_series"].append({
                    "symbol": row[0],
                    "datetime": str(row[1]) if row[1] else None,
                    "indicators": {
                        "close": str(row[5]) if row[5] else "N/A",
                        "vwap": str(row[7]) if row[7] else "N/A",
                        "bbi": str(row[8]) if row[8] else "N/A",
                        "bbiboll_upper": str(row[9]) if row[9] else "N/A",
                        "bbiboll_lower": str(row[10]) if row[10] else "N/A",
                        "bbiboll_ratio": str(row[11]) if row[11] else "N/A"
                    }
                })
            logger.debug(f"📊 {symbol}: 加载 {len(daily_result)} 条日线数据")
            
    except Exception as e:
        logger.warning(f"⚠️ {symbol} 历史数据查询失败：{e}")
    finally:
        hist_session.close()
        hist_engine.dispose()
    
    return result


def format_momentum_prompt(symbol: str, momentum: Dict[str, bool], ref_level: str) -> str:
    """
    格式化动量评估为 Prompt 可注入的字符串
    
    输出格式（与 prompt_template.txt 一致）：
    "{symbol}":{"5-min level strong momentum: True/False; 
                1-hour level strong momentum: True/False;
                1-day level strong momentum: True/False;
                1-week level strong momentum: True/False;
                Most suitable reference level: 5-min/1-hour/1-day/1-week"}
    """
    return f'''"{symbol}":{{"5-min level strong momentum: {str(momentum.get("5-min", False))}; 
                1-hour level strong momentum: {str(momentum.get("1-hour", False))};
                2-hour level strong momentum: {str(momentum.get("2-hour", False))};
                1-day level strong momentum: {str(momentum.get("1-day", False))};
                1-week level strong momentum: {str(momentum.get("1-week", False))};
                Most suitable reference level: {ref_level}"}}'''


def format_series_for_prompt(series: List[Dict], max_bars: int = None) -> str:
    """
    格式化时间序列数据为 Prompt 可读字符串
    限制 Token 用量，只保留最新 N 条
    """
    if not series:
        return "N/A"
    
    if max_bars:
        series = series[-max_bars:]  # 取最新 N 条
    
    lines = []
    for item in series:
        # 提取关键字段，避免冗余
        dt = item.get("datetime", "N/A")
        indicators = item.get("indicators", {})
        line = f"{dt} | close:{indicators.get('close','N/A')} vwap:{indicators.get('vwap','N/A')} bbi:{indicators.get('bbi','N/A')} ratio:{indicators.get('bbiboll_ratio','N/A')}"
        lines.append(line)
    
    return "\n".join(lines)


def build_daily_pool_context(pool_config: List[Dict], adapter: IBKRPriceAdapter) -> Dict[str, Any]:
    """
    构建完整的盘前上下文数据
    
    返回：
    {
        "est_time": "2026-03-28 09:00:00 EST",
        "daily_stock_pool": {  # 动量评估字典
            "AAPL": {...},
            ...
        },
        "stock_data": {  # 每只股票的多粒度数据
            "AAPL": {
                "5min_series": [...],
                "hourly_series": [...],
                "daily_series": [...],
                "latest": {...}
            },
            ...
        },
        "prompt_ready": {  # 直接可注入 Prompt 的格式化字符串
            "daily_stock_pool_str": "...",
            "AAPL_data_str": "...",
            ...
        }
    }
    """
    context = {
        "est_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S EST"),
        "daily_stock_pool": {},
        "stock_data": {},
        "prompt_ready": {}
    }
    
    # 1. 构建动量评估字典（供 Prompt 直接注入）
    pool_str_parts = []
    for stock in pool_config:
        symbol = stock["symbol"]
        momentum = stock["momentum"]
        ref_level = stock["reference_level"]
        
        # 存储原始字典（供程序逻辑使用）
        context["daily_stock_pool"][symbol] = {
            "momentum": momentum,
            "reference_level": ref_level,
            "notes": stock.get("notes", "")
        }
        
        # 格式化 Prompt 字符串
        pool_str_parts.append(format_momentum_prompt(symbol, momentum, ref_level))
    
    context["prompt_ready"]["daily_stock_pool_str"] = "\n".join(pool_str_parts)
    
    # 2. 查询每只股票的多粒度数据
    for stock in pool_config:
        symbol = stock["symbol"]
        logger.info(f"🔍 获取数据: {symbol}")
        
        data = query_multi_granularity_data(symbol, adapter)
        context["stock_data"][symbol] = data
        
        # 格式化序列数据为 Prompt 字符串
        series_5min = format_series_for_prompt(
            data.get("5min_series", []), 
            settings.MAX_BARS_5MIN
        )
        # hourly/daily 同理，需要根据实际数据源实现
        
        context["prompt_ready"][f"{symbol}_5min_series"] = series_5min
    
    logger.info(f"✅ 上下文构建完成: {len(context['stock_data'])} 只股票")
    return context


def save_context_cache(context: Dict, date_str: str = None) -> str:
    """保存上下文到缓存文件"""
    if not date_str:
        date_str = datetime.now().strftime("%Y%m%d")
    
    cache_dir = PROJECT_ROOT / "cache"
    cache_dir.mkdir(exist_ok=True)
    
    cache_path = cache_dir / f"context_{date_str}.json"
    
    # 序列化时处理 datetime 对象
    def default_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(context, f, indent=2, default=default_serializer, ensure_ascii=False)
    
    logger.info(f"💾 上下文缓存保存: {cache_path}")
    return str(cache_path)


def main(pool_path: str = None, output_cache: bool = True):
    """主入口"""
    logger.info("🚀 开始构建盘前资产池上下文")
    start_time = datetime.now()
    
    # 1. 确定模板路径
    if pool_path is None:
        # 默认使用今日模板
        today = datetime.now().strftime("%Y%m%d")
        pool_path = str(PROJECT_ROOT / "config" / f"stock_pool_{today}.txt")
        if not os.path.exists(pool_path):
            pool_path = str(PROJECT_ROOT / "config" / "stock_pool_template.txt")
    
    # 2. 解析资产池
    pool_config = parse_stock_pool_template(pool_path)
    if not pool_config:
        logger.error("❌ 资产池为空，退出")
        return None
    
    # 3. 初始化适配器
    adapter = IBKRPriceAdapter()
    
    # 4. 构建上下文
    context = build_daily_pool_context(pool_config, adapter)
    
    # 5. 保存缓存（可选）
    if output_cache:
        cache_path = save_context_cache(context)
        print(f"✅ 缓存路径: {cache_path}")
    
    # 6. 清理
    adapter.disconnect()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ 盘前准备完成，耗时 {elapsed:.1f}s")
    
    return context


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="构建盘前资产池上下文")
    parser.add_argument("--pool", type=str, help="资产池模板路径")
    parser.add_argument("--no-cache", action="store_true", help="不保存缓存文件")
    parser.add_argument("--log-level", type=str, default="INFO", 
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # 配置日志
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 运行
    result = main(
        pool_path=args.pool,
        output_cache=not args.no_cache
    )
    
    if result:
        print(f"\n📊 摘要: {len(result['stock_data'])} 只股票数据就绪")
    else:
        print("\n❌ 构建失败")
        sys.exit(1)