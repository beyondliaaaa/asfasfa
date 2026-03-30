#!/usr/bin/env python3
"""
IBKR 美股多粒度历史数据批量获取脚本
粒度：日线、周线、1 小时、2 小时、5 分钟
指标：close, vwap, bbi, bbiboll(10,3), bbiboll_ratio
"""
import pandas as pd
import numpy as np
from ib_insync import IB, Stock, util
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, UniqueConstraint, Index
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import os

# ================= 配置区域 =================
IB_HOST = os.getenv('IBKR_HOST', '127.0.0.1')
IB_PORT = int(os.getenv('IBKR_PORT', '4002'))
IB_CLIENT_ID = int(os.getenv('IBKR_CLIENT_ID', '11'))
DB_PATH = os.getenv('IBKR_DB_PATH', 'ibkr_us_stocks.db')

# 数据获取参数
DURATION_DAILY = '2 Y'      # 日线：2 年
DURATION_HOURLY = '6 M'     # 小时线：6 个月
DURATION_5MIN = '1 M'       # 5 分钟：1 个月（数据量大，建议短期）
WHAT_TO_SHOW = 'TRADES'
USE_RTH = True

# 批量处理参数
BATCH_SIZE = 20             # 多粒度获取时降低批次大小
RATE_LIMIT_DELAY = 1.0      # 增加延迟避免限流
MAX_RETRIES = 3

# 粒度配置：是否启用各粒度
ENABLE_DAILY = True
ENABLE_WEEKLY = True
ENABLE_HOURLY = True
ENABLE_2HOUR = True         # ✅ 新增：2 小时级别
ENABLE_5MIN = True

# 股票代码来源
STOCK_LIST_SOURCE = 'fallback'
FALLBACK_SYMBOLS = [
    'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'TSLA', 'META', 'NVDA'
]

# ===========================================

# ================= 数据库引擎 =================
engine = create_engine(f'sqlite:///{DB_PATH}', pool_pre_ping=True, echo=False)
Base = declarative_base()

# ================= 数据库模型 =================
class DailyBar(Base):
    __tablename__ = 'daily_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    date = Column(DateTime, nullable=False)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    
    # 技术指标
    vwap = Column(Float)
    bbi = Column(Float)
    bbiboll_upper = Column(Float)
    bbiboll_lower = Column(Float)
    bbiboll_ratio = Column(Float)
    
    adj_close = Column(Float)
    bar_size = Column(String(10), default='1 day')
    
    __table_args__ = (
        UniqueConstraint('symbol', 'date', name='uix_symbol_date'),
        Index('idx_symbol_daily', 'symbol'),
        Index('idx_date_daily', 'date'),
    )


class WeeklyBar(Base):
    __tablename__ = 'weekly_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    week_start_date = Column(DateTime, nullable=False)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    
    # 技术指标
    vwap = Column(Float)
    bbi = Column(Float)
    bbiboll_upper = Column(Float)
    bbiboll_lower = Column(Float)
    bbiboll_ratio = Column(Float)
    
    adj_close = Column(Float)
    bar_size = Column(String(10), default='1 week')
    
    __table_args__ = (
        UniqueConstraint('symbol', 'week_start_date', name='uix_symbol_week'),
        Index('idx_symbol_weekly', 'symbol'),
    )


class HourlyBar(Base):
    __tablename__ = 'hourly_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    datetime = Column(DateTime, nullable=False, index=True)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    
    # 技术指标
    vwap = Column(Float)
    bbi = Column(Float)
    bbiboll_upper = Column(Float)
    bbiboll_lower = Column(Float)
    bbiboll_ratio = Column(Float)
    
    bar_size = Column(String(10), default='1 hour')
    
    __table_args__ = (
        UniqueConstraint('symbol', 'datetime', name='uix_symbol_hourly'),
        Index('idx_symbol_hourly', 'symbol'),
        Index('idx_datetime_hourly', 'datetime'),
    )


# ========== 新增：2 小时级别 ==========
class Hourly2Bar(Base):
    __tablename__ = 'hourly_2hour_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    datetime = Column(DateTime, nullable=False, index=True)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    
    # 技术指标
    vwap = Column(Float)
    bbi = Column(Float)
    bbiboll_upper = Column(Float)
    bbiboll_lower = Column(Float)
    bbiboll_ratio = Column(Float)
    
    bar_size = Column(String(10), default='2 hour')
    
    __table_args__ = (
        UniqueConstraint('symbol', 'datetime', name='uix_symbol_2hour'),
        Index('idx_symbol_2hour', 'symbol'),
        Index('idx_datetime_2hour', 'datetime'),
    )


class Intraday5minBar(Base):
    __tablename__ = 'intraday_5min_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    datetime = Column(DateTime, nullable=False, index=True)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    
    # 技术指标
    vwap = Column(Float)
    bbi = Column(Float)
    bbiboll_upper = Column(Float)
    bbiboll_lower = Column(Float)
    bbiboll_ratio = Column(Float)
    
    bar_size = Column(String(10), default='5 mins')
    
    __table_args__ = (
        UniqueConstraint('symbol', 'datetime', name='uix_symbol_5min'),
        Index('idx_symbol_5min', 'symbol'),
        Index('idx_datetime_5min', 'datetime'),
    )


# 创建所有表
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


# ================= 技术指标计算 =================
def calculate_vwap(df: pd.DataFrame) -> pd.Series:
    """
    VWAP = Σ(typical_price * volume) / Σ(volume)
    typical_price = (high + low + close) / 3
    """
    if df.empty or 'volume' not in df.columns:
        return pd.Series(dtype=float)
    
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    cumulative_tp_vol = (typical_price * df['volume']).cumsum()
    cumulative_vol = df['volume'].cumsum()
    
    with np.errstate(divide='ignore', invalid='ignore'):
        vwap = cumulative_tp_vol / cumulative_vol
        vwap = vwap.replace([np.inf, -np.inf], np.nan)
    
    return vwap


def calculate_bbi(df: pd.DataFrame) -> pd.Series:
    """
    BBI = (MA3 + MA6 + MA12 + MA24) / 4
    """
    if df.empty:
        return pd.Series(dtype=float)
    
    ma3 = df['close'].rolling(window=3).mean()
    ma6 = df['close'].rolling(window=6).mean()
    ma12 = df['close'].rolling(window=12).mean()
    ma24 = df['close'].rolling(window=24).mean()
    bbi = (ma3 + ma6 + ma12 + ma24) / 4
    
    return bbi


def calculate_bbiboll(bbi: pd.Series, period: int = 10, std_dev: int = 3) -> tuple:
    """
    BBI Bollinger Bands
    上轨 = BBI + std_dev * std(BBI, period)
    下轨 = BBI - std_dev * std(BBI, period)
    """
    if bbi.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    
    bbi_std = bbi.rolling(window=period).std()
    bbiboll_upper = bbi + std_dev * bbi_std
    bbiboll_lower = bbi - std_dev * bbi_std
    
    return bbiboll_upper, bbiboll_lower


def calculate_bbiboll_ratio(bbiboll_upper: pd.Series, bbi: pd.Series) -> pd.Series:
    """bbiboll_upper / bbi"""
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
    bbiboll_upper, bbiboll_lower = calculate_bbiboll(df['bbi'])
    df['bbiboll_upper'] = bbiboll_upper
    df['bbiboll_lower'] = bbiboll_lower
    
    # BBI Bollinger Ratio
    df['bbiboll_ratio'] = calculate_bbiboll_ratio(df['bbiboll_upper'], df['bbi'])
    
    return df


# ================= 数据获取 =================
def get_all_symbols(method=None):
    """获取股票代码列表（简化版）"""
    if method == 'fallback' or STOCK_LIST_SOURCE == 'fallback':
        symbols = FALLBACK_SYMBOLS.copy()
    else:
        symbols = FALLBACK_SYMBOLS.copy()
    
    final_symbols = sorted([
        s for s in symbols
        if s and isinstance(s, str) and s.isalnum() and 1 <= len(s) <= 5
    ])
    
    print(f"📊 有效股票代码：{len(final_symbols)} 只")
    return final_symbols


def connect_ibkr():
    """连接 IBKR 网关"""
    ib = IB()
    try:
        ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=30)
        print(f"✅ 连接成功：{IB_HOST}:{IB_PORT}")
        return ib
    except Exception as e:
        print(f"❌ 连接失败：{e}")
        return None


def fetch_historical_data(ib, contract, duration, bar_size, retries=MAX_RETRIES):
    """获取历史数据（带重试）"""
    for attempt in range(retries):
        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=WHAT_TO_SHOW,
                useRTH=USE_RTH,
                formatDate=1
            )
            
            if not bars:
                if attempt < retries - 1:
                    print(f"  ⚠️ 无数据，重试 {attempt+1}/{retries}")
                    time.sleep(2 ** attempt)
                    continue
                return pd.DataFrame()
            
            df = util.df(bars)
            if df.empty:
                return pd.DataFrame()
            
            # 处理日期列
            df['date'] = pd.to_datetime(df['date'])
            
            # 保留必要列
            cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            df = df[[c for c in cols if c in df.columns]]
            
            # 处理时区
            if not df.empty and pd.api.types.is_datetime64_any_dtype(df['date']):
                if df['date'].dt.tz is not None:
                    df['date'] = df['date'].dt.tz_localize(None)
            
            return df
            
        except Exception as e:
            if attempt < retries - 1:
                print(f"  ⚠️ 错误：{e}, 重试 {attempt+1}/{retries}")
                time.sleep(2 ** attempt)
            else:
                print(f"  ❌ 获取失败：{e}")
            return pd.DataFrame()
    
    return pd.DataFrame()


# ================= 数据重采样 =================
def resample_to_weekly(daily_df: pd.DataFrame) -> pd.DataFrame:
    """日线 → 周线"""
    if daily_df.empty:
        return pd.DataFrame()
    
    df = daily_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    weekly = df.resample('W-MON').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    weekly.reset_index(inplace=True)
    weekly.rename(columns={'date': 'week_start_date'}, inplace=True)
    weekly['adj_close'] = weekly['close']
    
    return weekly


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


# ========== 新增：5 分钟 → 2 小时 ==========
def resample_to_2hour(df_5min: pd.DataFrame) -> pd.DataFrame:
    """5 分钟 → 2 小时"""
    if df_5min.empty:
        return pd.DataFrame()
    
    df = df_5min.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    
    # 2 小时重采样
    hourly_2 = df.resample('2H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    hourly_2.reset_index(inplace=True)
    return hourly_2


# ========== 新增：1 小时 → 2 小时（备选方案）==========
def resample_hourly_to_2hour(df_hourly: pd.DataFrame) -> pd.DataFrame:
    """1 小时 → 2 小时（当没有 5 分钟数据时使用）"""
    if df_hourly.empty:
        return pd.DataFrame()
    
    df = df_hourly.copy()
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    
    hourly_2 = df.resample('2H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    hourly_2.reset_index(inplace=True)
    return hourly_2


# ================= 数据库存储 =================
def check_exists(session, table_class, symbol, date_col, date_value):
    """检查记录是否已存在"""
    kwargs = {'symbol': symbol, date_col: date_value}
    return session.query(table_class).filter_by(**kwargs).first() is not None


def save_bars(session, df: pd.DataFrame, symbol: str, table_class, date_col: str):
    """保存数据到数据库"""
    if df.empty:
        return 0
    
    count = 0
    for _, row in df.iterrows():
        try:
            record = row.to_dict()
            
            # 转换时间类型
            if isinstance(record[date_col], pd.Timestamp):
                record[date_col] = record[date_col].to_pydatetime()
            
            # 检查是否存在
            if check_exists(session, table_class, symbol, date_col, record[date_col]):
                continue
            
            # 创建对象
            obj = table_class(
                symbol=symbol,
                **{k: v for k, v in record.items() if k != 'symbol'}
            )
            session.add(obj)
            count += 1
            
        except Exception as e:
            print(f"    ⚠️ 保存失败：{e}")
            continue
    
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"  ❌ 提交失败：{e}")
    
    return count


# ================= 主流程 =================
def process_symbol(ib, session, symbol: str):
    """处理单只股票的所有粒度数据"""
    print(f"\n🔄 处理：{symbol}")
    contract = Stock(symbol, 'SMART', 'USD')
    stats = {}
    
    # ── 日线 ──
    if ENABLE_DAILY:
        print(f"  📊 获取日线 ({DURATION_DAILY})...")
        daily_df = fetch_historical_data(ib, contract, DURATION_DAILY, '1 day')
        if not daily_df.empty:
            daily_df = calculate_indicators(daily_df)
            daily_count = save_bars(session, daily_df, symbol, DailyBar, 'date')
            stats['daily'] = daily_count
            print(f"    ✅ 日线：{daily_count} 条")
            
            if ENABLE_WEEKLY:
                weekly_df = resample_to_weekly(daily_df)
                if not weekly_df.empty:
                    weekly_df = calculate_indicators(weekly_df)
                    weekly_count = save_bars(session, weekly_df, symbol, WeeklyBar, 'week_start_date')
                    stats['weekly'] = weekly_count
                    print(f"    ✅ 周线：{weekly_count} 条")
        else:
            print(f"    ⚠️ 日线无数据")
    
    # ── 1 小时线 ──
    if ENABLE_HOURLY:
        print(f"  📊 获取小时线 ({DURATION_HOURLY})...")
        hourly_df = fetch_historical_data(ib, contract, DURATION_HOURLY, '1 hour')
        if not hourly_df.empty:
            # 🔧 关键修复：重命名时间列
            hourly_df = hourly_df.rename(columns={'date': 'datetime'})
            hourly_df = calculate_indicators(hourly_df)
            hourly_count = save_bars(session, hourly_df, symbol, HourlyBar, 'datetime')
            stats['hourly'] = hourly_count
            print(f"    ✅ 小时线：{hourly_count} 条")
        else:
            print(f"    ⚠️ 小时线无数据")
    
    # ── 2 小时线 (新增) ──
    if ENABLE_2HOUR:
        print(f"  📊 生成 2 小时线 (从重采样)...")
        # 优先从 5 分钟数据重采样，其次从 1 小时数据重采样
        df_2hour = pd.DataFrame()
        
        # 尝试从 5 分钟数据生成（如果有）
        if ENABLE_5MIN and '5min_df' in locals() and not min5_df.empty:
            df_2hour = resample_to_2hour(min5_df)
        
        # 如果 5 分钟数据不可用，从 1 小时数据生成
        if df_2hour.empty and not hourly_df.empty:
            df_2hour = resample_hourly_to_2hour(hourly_df)
        
        if not df_2hour.empty:
            df_2hour = calculate_indicators(df_2hour)
            df_2hour['bar_size'] = '2 hour'
            count_2hour = save_bars(session, df_2hour, symbol, Hourly2Bar, 'datetime')
            stats['2hour'] = count_2hour
            print(f"    ✅ 2 小时线：{count_2hour} 条")
        else:
            print(f"    ⚠️ 2 小时线无源数据")
    
    # ── 5 分钟线 ──
    if ENABLE_5MIN:
        print(f"  📊 获取 5 分钟线 ({DURATION_5MIN})...")
        min5_df = fetch_historical_data(ib, contract, DURATION_5MIN, '5 mins')
        if not min5_df.empty:
            # 🔧 关键修复：重命名时间列
            min5_df = min5_df.rename(columns={'date': 'datetime'})
            min5_df = calculate_indicators(min5_df)
            min5_count = save_bars(session, min5_df, symbol, Intraday5minBar, 'datetime')
            stats['5min'] = min5_count
            print(f"    ✅ 5 分钟线：{min5_count} 条")
        else:
            print(f"    ⚠️ 5 分钟线无数据")
    
    return stats


def main():
    print("=" * 70)
    print("🚀 IBKR 美股多粒度历史数据批量获取")
    print(f"   粒度：日线{'✅' if ENABLE_DAILY else '❌'} | "
          f"周线{'✅' if ENABLE_WEEKLY else '❌'} | "
          f"小时线{'✅' if ENABLE_HOURLY else '❌'} | "
          f"2 小时{'✅' if ENABLE_2HOUR else '❌'} | "
          f"5 分钟{'✅' if ENABLE_5MIN else '❌'}")
    print(f"   指标：close, vwap, bbi, bbiboll(10,3), bbiboll_ratio")
    print("=" * 70)
    
    # 1. 连接 IBKR
    ib = connect_ibkr()
    if not ib:
        return
    
    # 2. 获取股票代码
    symbols = get_all_symbols(STOCK_LIST_SOURCE)
    if not symbols:
        print("❌ 无可用股票代码")
        ib.disconnect()
        return
    
    # 3. 过滤已存在的股票（按日线判断）
    session = Session()
    print("\n🔍 检查已存在的股票（按日线）...")
    new_symbols = []
    for symbol in tqdm(symbols, desc="Checking"):
        exists = session.query(DailyBar).filter_by(symbol=symbol).first()
        if not exists:
            new_symbols.append(symbol)
    
    print(f"✅ 已存在：{len(symbols) - len(new_symbols)} | 🆕 待获取：{len(new_symbols)}")
    
    if not new_symbols:
        print("💡 所有股票日线数据已存在")
        session.close()
        ib.disconnect()
        return
    
    # 4. 批量处理
    print(f"\n📥 开始获取 (批次大小：{BATCH_SIZE})")
    total_stats = {'daily': 0, 'weekly': 0, 'hourly': 0, '2hour': 0, '5min': 0}
    success_count = 0
    
    for i in range(0, len(new_symbols), BATCH_SIZE):
        batch = new_symbols[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(new_symbols) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"\n{'='*70}")
        print(f"批次 {batch_num}/{total_batches}")
        print(f"{'='*70}")
        
        for symbol in tqdm(batch, desc=f"Batch {batch_num}"):
            try:
                stats = process_symbol(ib, session, symbol)
                for k, v in stats.items():
                    total_stats[k] = total_stats.get(k, 0) + v
                
                if any(v > 0 for v in stats.values()):
                    success_count += 1
                
                # 速率限制
                time.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                print(f"  ❌ [{symbol}] 错误：{e}")
                session.rollback()
                time.sleep(2)
        
        # 批次结束提交
        session.commit()
        print(f"\n💤 批次完成，休息 3 秒...")
        time.sleep(3)
    
    # 5. 总结
    print(f"\n{'='*70}")
    print("📊 任务完成")
    print(f"{'='*70}")
    print(f"总股票：{len(symbols)} | 新获取：{len(new_symbols)} | 成功：{success_count}")
    
    if ENABLE_DAILY:
        print(f"日线记录：{total_stats['daily']}")
    if ENABLE_WEEKLY:
        print(f"周线记录：{total_stats['weekly']}")
    if ENABLE_HOURLY:
        print(f"小时线记录：{total_stats['hourly']}")
    if ENABLE_2HOUR:
        print(f"2 小时线记录：{total_stats['2hour']}")
    if ENABLE_5MIN:
        print(f"5 分钟记录：{total_stats['5min']}")
    
    print(f"数据库：{DB_PATH}")
    print(f"{'='*70}")
    
    session.close()
    ib.disconnect()
    print("\n✅ 连接已关闭")


if __name__ == "__main__":
    main()