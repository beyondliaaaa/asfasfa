#!/usr/bin/env python3
"""
盘中 intraday 数据库更新模块
功能：
1. 从 IBKR 获取最新 5 分钟 K 线
2. 重采样生成 1 小时、2 小时 K 线
3. 计算技术指标并更新数据库

运行时机：盘中每 5 分钟
"""
import pandas as pd
import numpy as np
from ib_insync import IB, Stock, util
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict

from config import settings
from shared.intraday_db import (
    get_session, IntradayBar, 
    Intraday2HourBar,  # 新增
    HourlyBar
)

logger = logging.getLogger(__name__)


def connect_ibkr() -> IB:
    """连接 IBKR"""
    ib = IB()
    try:
        ib.connect(
            host=settings.IB_HOST,
            port=settings.IB_PORT,
            clientId=settings.IB_CLIENT_ID,
            timeout=10
        )
        logger.info(f"✅ IBKR 连接成功：{settings.IB_HOST}:{settings.IB_PORT}")
        return ib
    except Exception as e:
        logger.error(f"❌ IBKR 连接失败：{e}")
        return None


def fetch_latest_5min_bars(ib: IB, symbol: str, num_bars: int = 50) -> pd.DataFrame:
    """获取最新 5 分钟 K 线"""
    contract = Stock(symbol, "SMART", "USD")
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='1 D',
            barSizeSetting='5 mins',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )
        
        if not bars:
            return pd.DataFrame()
        
        df = util.df(bars)
        if df.empty:
            return pd.DataFrame()
        
        # 处理时区
        df['datetime'] = pd.to_datetime(df['date'])
        if df['datetime'].dt.tz is not None:
            df['datetime'] = df['datetime'].dt.tz_localize(None)
        
        # 保留必要列
        cols = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        df = df[[c for c in cols if c in df.columns]]
        
        return df.tail(num_bars)
        
    except Exception as e:
        logger.error(f"获取 {symbol} 数据失败：{e}")
        return pd.DataFrame()


def resample_to_2hour(df_5min: pd.DataFrame) -> pd.DataFrame:
    """5 分钟 → 2 小时"""
    if df_5min.empty:
        return pd.DataFrame()
    
    df = df_5min.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    
    # 2 小时重采样
    hourly = df.resample('2H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    hourly.reset_index(inplace=True)
    return hourly


def resample_to_hourly(df_5min: pd.DataFrame) -> pd.DataFrame:
    """5 分钟 → 1 小时"""
    if df_5min.empty:
        return pd.DataFrame()
    
    df = df_5min.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    
    hourly = df.resample('1H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    hourly.reset_index(inplace=True)
    return hourly


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算技术指标"""
    if df.empty:
        return df
    
    df = df.copy()
    
    # VWAP
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    cumulative_tp_vol = (typical_price * df['volume']).cumsum()
    cumulative_vol = df['volume'].cumsum()
    df['vwap'] = cumulative_tp_vol / cumulative_vol
    
    # BBI
    ma3 = df['close'].rolling(window=3).mean()
    ma6 = df['close'].rolling(window=6).mean()
    ma12 = df['close'].rolling(window=12).mean()
    ma24 = df['close'].rolling(window=24).mean()
    df['bbi'] = (ma3 + ma6 + ma12 + ma24) / 4
    
    # BBI Bollinger Bands
    bbi_std = df['bbi'].rolling(window=10).std()
    df['bbiboll_upper'] = df['bbi'] + 3 * bbi_std
    df['bbiboll_lower'] = df['bbi'] - 3 * bbi_std
    
    # BBI Bollinger Ratio
    with np.errstate(divide='ignore', invalid='ignore'):
        df['bbiboll_ratio'] = df['bbiboll_upper'] / df['bbi']
        df['bbiboll_ratio'] = df['bbiboll_ratio'].replace([np.inf, -np.inf], np.nan)
    
    return df


def update_intraday_bars(session, df: pd.DataFrame, symbol: str, bar_size: str = '5 min'):
    """更新 intraday 数据库"""
    if df.empty:
        return 0
    
    # 根据 bar_size 选择表
    if bar_size == '5 min':
        TableClass = IntradayBar
        datetime_col = 'datetime'
    elif bar_size == '2 hour':
        TableClass = Intraday2HourBar
        datetime_col = 'datetime'
    elif bar_size == '1 hour':
        TableClass = HourlyBar
        datetime_col = 'datetime'
    else:
        logger.error(f"未知的 bar_size: {bar_size}")
        return 0
    
    now = datetime.now()
    count = 0
    
    for _, row in df.iterrows():
        # 检查是否已存在
        exists = session.query(TableClass).filter_by(
            symbol=symbol,
            **{datetime_col: row['datetime']}
        ).first()
        
        if exists:
            # 更新
            exists.open = row['open']
            exists.high = row['high']
            exists.low = row['low']
            exists.close = row['close']
            exists.volume = int(row['volume']) if row['volume'] else 0
            exists.vwap = row.get('vwap')
            exists.bbi = row.get('bbi')
            exists.bbiboll_upper = row.get('bbiboll_upper')
            exists.bbiboll_lower = row.get('bbiboll_lower')
            exists.bbiboll_ratio = row.get('bbiboll_ratio')
            exists.update_time = now
        else:
            # 插入
            bar = TableClass(
                symbol=symbol,
                **{datetime_col: row['datetime']},
                open=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                volume=int(row['volume']) if row['volume'] else 0,
                vwap=row.get('vwap'),
                bbi=row.get('bbi'),
                bbiboll_upper=row.get('bbiboll_upper'),
                bbiboll_lower=row.get('bbiboll_lower'),
                bbiboll_ratio=row.get('bbiboll_ratio'),
                update_time=now
            )
            session.add(bar)
            count += 1
    
    try:
        session.commit()
        logger.info(f"💾 {symbol} [{bar_size}]: 更新 {count} 条记录")
    except Exception as e:
        session.rollback()
        logger.error(f"数据库写入失败：{e}")
        return 0
    
    return count


def update_symbol(ib: IB, symbol: str) -> Dict[str, int]:
    """更新单只股票的所有粒度数据"""
    session = get_session()
    stats = {}
    
    try:
        # 1. 获取 5 分钟数据
        df_5min = fetch_latest_5min_bars(ib, symbol)
        if not df_5min.empty:
            df_5min = calculate_indicators(df_5min)
            stats['5min'] = update_intraday_bars(session, df_5min, symbol, '5 min')
            
            # 2. 重采样到 1 小时
            df_hourly = resample_to_hourly(df_5min)
            if not df_hourly.empty:
                df_hourly = calculate_indicators(df_hourly)
                stats['1hour'] = update_intraday_bars(session, df_hourly, symbol, '1 hour')
            
            # 3. 重采样到 2 小时
            df_2hour = resample_to_2hour(df_5min)
            if not df_2hour.empty:
                df_2hour = calculate_indicators(df_2hour)
                stats['2hour'] = update_intraday_bars(session, df_2hour, symbol, '2 hour')
    
    except Exception as e:
        logger.error(f"{symbol} 更新失败：{e}")
        session.rollback()
    finally:
        session.close()
    
    return stats


def main(symbols: List[str] = None):
    """主入口"""
    logger.info("🚀 开始更新 intraday 数据库")
    start_time = datetime.now()
    
    # 1. 连接 IBKR
    ib = connect_ibkr()
    if not ib:
        return
    
    # 2. 确定股票列表
    if symbols is None:
        # 从资产池读取（需要实现）
        symbols = ['AAPL', 'MSFT', 'TSLA']  # 默认
    
    # 3. 更新每只股票
    total_stats = {'5min': 0, '1hour': 0, '2hour': 0}
    for symbol in symbols:
        logger.info(f"🔄 更新：{symbol}")
        stats = update_symbol(ib, symbol)
        for k, v in stats.items():
            total_stats[k] = total_stats.get(k, 0) + v
        
        # 避免 API 限流
        time.sleep(0.5)
    
    # 4. 清理
    ib.disconnect()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ 更新完成 | 5min:{total_stats['5min']} 1hour:{total_stats['1hour']} 2hour:{total_stats['2hour']} | 耗时 {elapsed:.1f}s")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="更新 intraday 数据库")
    parser.add_argument("--symbols", type=str, nargs="+", help="股票列表")
    parser.add_argument("--log-level", type=str, default="INFO")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    main(symbols=args.symbols)