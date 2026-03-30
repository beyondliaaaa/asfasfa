#!/usr/bin/env python3
"""
构建实时 intraday 数据库
- 从 IBKR 获取当日 5 分钟 K 线
- 计算技术指标：close, vwap, bbi, bbiboll(10,3)
- 存入 intraday DB

运行时机：
- 盘前：构建当日历史部分
- 盘中：定时刷新（每 5 分钟）
"""
import pandas as pd
import numpy as np
from ib_insync import IB, Stock, util
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import time
import os
import logging

from agent_tools.intraday_db import IntradayBar, engine, get_session

# ================= 配置 =================
IB_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IBKR_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", "22"))

HISTORICAL_DB_PATH = os.getenv("IBKR_DB_PATH", "ibkr_us_stocks.db")

# 实时数据参数
BAR_SIZE = '5 mins'
DURATION = '1 D'  # 获取当日数据
WHAT_TO_SHOW = 'TRADES'
USE_RTH = True

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_ibkr():
    """连接 IBKR"""
    ib = IB()
    try:
        ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)
        logger.info(f"✅ 连接 IBKR 成功：{IB_HOST}:{IB_PORT}")
        return ib
    except Exception as e:
        logger.error(f"❌ 连接失败：{e}")
        return None

def get_tradeable_symbols() -> list:
    """
    从历史数据库获取可交易资产池
    逻辑：过去 20 日有交易、流动性良好的股票
    """
    from sqlalchemy import create_engine, text
    
    if not os.path.exists(HISTORICAL_DB_PATH):
        logger.warning(f"历史数据库不存在：{HISTORICAL_DB_PATH}，使用默认列表")
        return ['AAPL', 'MSFT', 'TSLA', 'NVDA', 'GOOGL']
    
    try:
        hist_engine = create_engine(f'sqlite:///{HISTORICAL_DB_PATH}')
        with hist_engine.connect() as conn:
            query = text("""
                SELECT DISTINCT symbol 
                FROM daily_bars 
                WHERE date >= date('now', '-20 days')
                AND volume > 1000000
                ORDER BY symbol
            """)
            result = conn.execute(query)
            symbols = [row[0] for row in result.fetchall()]
            logger.info(f"📋 从历史数据库获取 {len(symbols)} 只可交易股票")
            return symbols if symbols else ['AAPL', 'MSFT', 'TSLA']
    except Exception as e:
        logger.error(f"查询历史数据库失败：{e}")
        return ['AAPL', 'MSFT', 'TSLA']

def fetch_intraday_data(ib, symbol: str) -> pd.DataFrame:
    """获取单只股票的当日 5 分钟 K 线"""
    contract = Stock(symbol, "SMART", "USD")
    
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr=DURATION,
            barSizeSetting=BAR_SIZE,
            whatToShow=WHAT_TO_SHOW,
            useRTH=USE_RTH,
            formatDate=1
        )
        
        if not bars:
            logger.warning(f"⚠️ {symbol} 未获取到数据")
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
        
        return df
        
    except Exception as e:
        logger.error(f"获取 {symbol} 数据失败：{e}")
        return pd.DataFrame()

def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """
    计算 VWAP (Volume Weighted Average Price)
    VWAP = Σ(typical_price * volume) / Σ(volume)
    typical_price = (high + low + close) / 3
    """
    if df.empty:
        return pd.Series()
    
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    cumulative_tp_vol = (typical_price * df['volume']).cumsum()
    cumulative_vol = df['volume'].cumsum()
    
    vwap = cumulative_tp_vol / cumulative_vol
    return vwap

def calculate_bbi(df: pd.DataFrame) -> pd.Series:
    """
    计算 BBI (Bull and Bear Index)
    BBI = (MA3 + MA6 + MA12 + MA24) / 4
    """
    if df.empty:
        return pd.Series()
    
    ma3 = df['close'].rolling(window=3).mean()
    ma6 = df['close'].rolling(window=6).mean()
    ma12 = df['close'].rolling(window=12).mean()
    ma24 = df['close'].rolling(window=24).mean()
    
    bbi = (ma3 + ma6 + ma12 + ma24) / 4
    return bbi

def calculate_bbiboll(df: pd.DataFrame, bbi: pd.Series, period: int = 10, std_dev: int = 3) -> tuple:
    """
    计算 BBI Bollinger Bands
    中轨 = BBI
    上轨 = BBI + std_dev * std(BBI, period)
    下轨 = BBI - std_dev * std(BBI, period)
    """
    if df.empty or bbi.empty:
        return pd.Series(), pd.Series()
    
    bbi_std = bbi.rolling(window=period).std()
    
    bbiboll_upper = bbi + std_dev * bbi_std
    bbiboll_lower = bbi - std_dev * bbi_std
    
    return bbiboll_upper, bbiboll_lower

def calculate_bbiboll_ratio(bbiboll_upper: pd.Series, bbi: pd.Series) -> pd.Series:
    """
    计算 bbiboll_upper / bbi 比值
    """
    with np.errstate(divide='ignore', invalid='ignore'):
        ratio = bbiboll_upper / bbi
        ratio = ratio.replace([np.inf, -np.inf], np.nan)
    return ratio

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算所有技术指标"""
    if df.empty:
        return df
    
    df = df.copy()
    
    # VWAP
    df['vwap'] = calculate_vwap(df)
    
    # BBI
    df['bbi'] = calculate_bbi(df)
    
    # BBI Bollinger Bands
    bbiboll_upper, bbiboll_lower = calculate_bbiboll(df, df['bbi'])
    df['bbiboll_upper'] = bbiboll_upper
    df['bbiboll_lower'] = bbiboll_lower
    
    # BBI Bollinger Ratio
    df['bbiboll_ratio'] = calculate_bbiboll_ratio(df['bbiboll_upper'], df['bbi'])
    
    return df

def save_to_intraday_db(session, df: pd.DataFrame, symbol: str):
    """存入 intraday 数据库"""
    if df.empty:
        return 0
    
    count = 0
    now = datetime.now()  # ✅ 在函数开头获取当前时间
    
    for _, row in df.iterrows():
        # 检查是否已存在（更新模式）
        exists = session.query(IntradayBar).filter_by(
            symbol=symbol, 
            datetime=row['datetime']
        ).first()
        
        if exists:
            # 更新现有记录
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
            exists.update_time = now  # ✅ 手动设置
        else:
            # 插入新记录
            bar = IntradayBar(
                symbol=symbol,
                datetime=row['datetime'],
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
                update_time=now  # ✅ 手动设置
            )
            session.add(bar)
            count += 1
    
    try:
        session.commit()
        logger.info(f"💾 {symbol}: 处理 {count} 条 5 分钟 K 线")
    except Exception as e:
        session.rollback()
        logger.error(f"数据库写入失败：{e}")
    
    return count

def main():
    logger.info("🚀 开始构建 intraday 数据库")
    start_time = datetime.now()
    
    # 1. 连接 IBKR
    ib = connect_ibkr()
    if not ib:
        return
    
    # 2. 获取可交易资产池
    symbols = get_tradeable_symbols()
    logger.info(f"📋 待处理股票：{symbols}")
    
    # 3. 获取并存储数据
    session = get_session()
    success_count = 0
    
    for symbol in symbols:
        try:
            # 获取数据
            df = fetch_intraday_data(ib, symbol)
            
            if not df.empty:
                # 计算指标
                df = calculate_indicators(df)
                
                # 存储
                save_to_intraday_db(session, df, symbol)
                success_count += 1
            
            # 避免 API 限流
            time.sleep(0.3)
            
        except Exception as e:
            logger.error(f"{symbol} 处理失败：{e}")
            session.rollback()
    
    session.close()
    ib.disconnect()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ intraday 数据库构建完成：{success_count}/{len(symbols)} 只股票成功，耗时 {elapsed:.1f}s")

if __name__ == "__main__":
    main()