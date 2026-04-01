#!/usr/bin/env python3
"""
资产池验证模块
功能：检查资产池中的股票是否在历史数据库中存在
"""
import logging
from pathlib import Path
from typing import List, Dict, Tuple
from sqlalchemy import create_engine, text

from config import settings

logger = logging.getLogger(__name__)


def check_symbols_in_database(symbols: List[str], db_path: str = None) -> Dict[str, bool]:
    """
    检查股票是否在历史数据库中存在
    
    Args:
        symbols: 股票代码列表
        db_path: 历史数据库路径（默认使用 settings.HISTORICAL_DB_PATH）
    
    Returns:
        {symbol: exists (bool)}
    """
    if db_path is None:
        db_path = settings.HISTORICAL_DB_PATH
    
    db_path = Path(db_path)
    if not db_path.exists():
        logger.warning(f"⚠️ 历史数据库不存在：{db_path}")
        return {s: False for s in symbols}
    
    engine = create_engine(f'sqlite:///{db_path}')
    results = {}
    
    try:
        with engine.connect() as conn:
            # 批量查询所有股票
            placeholders = ','.join([f':sym{i}' for i in range(len(symbols))])
            query = text(f"""
                SELECT DISTINCT symbol 
                FROM daily_bars 
                WHERE symbol IN ({placeholders})
            """)
            params = {f'sym{i}': s.upper() for i, s in enumerate(symbols)}
            
            result = conn.execute(query, params)
            existing_symbols = {row[0] for row in result.fetchall()}
            
            for symbol in symbols:
                results[symbol.upper()] = symbol.upper() in existing_symbols
                
    except Exception as e:
        logger.error(f"数据库查询失败：{e}")
        results = {s: False for s in symbols}
    finally:
        engine.dispose()
    
    return results


def validate_stock_pool(pool_config: List[Dict], warn_missing: bool = True) -> Tuple[List[Dict], List[str]]:
    """
    验证资产池中的股票
    
    Args:
        pool_config: 解析后的资产池配置
        warn_missing: 是否警告缺失的股票
    
    Returns:
        (valid_pool, missing_symbols)
        - valid_pool: 有效的股票配置列表
        - missing_symbols: 缺失的股票列表
    """
    symbols = [s["symbol"] for s in pool_config]
    logger.info(f"🔍 验证 {len(symbols)} 只股票是否在历史数据库中...")
    
    # 检查股票是否存在
    existence = check_symbols_in_database(symbols)
    
    # 分离有效和缺失的股票
    valid_pool = []
    missing_symbols = []
    
    for stock in pool_config:
        symbol = stock["symbol"]
        if existence.get(symbol, False):
            valid_pool.append(stock)
        else:
            missing_symbols.append(symbol)
            if warn_missing:
                logger.warning(f"⚠️ {symbol} 在历史数据库中不存在，将跳过")
    
    # 汇总报告
    logger.info(f"✅ 有效股票：{len(valid_pool)} | ⚠️ 缺失股票：{len(missing_symbols)}")
    
    if missing_symbols:
        logger.warning(f"缺失股票列表：{', '.join(missing_symbols)}")
    
    return valid_pool, missing_symbols