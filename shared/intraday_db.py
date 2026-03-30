#!/usr/bin/env python3
"""
实时 intraday 数据库 Schema
粒度：5 分钟 + 1 小时 + 2 小时 K 线 + 技术指标
"""
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, UniqueConstraint, Index
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import os

DB_PATH = os.getenv("INTRADAY_DB_PATH", "ibkr_intraday.db")
engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
Base = declarative_base()


class IntradayBar(Base):
    """5 分钟级 K 线 + 技术指标"""
    __tablename__ = 'intraday_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    datetime = Column(DateTime, nullable=False, index=True)
    
    # OHLCV
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
    
    bar_size = Column(String(10), default='5 min')
    update_time = Column(DateTime)
    
    __table_args__ = (
        UniqueConstraint('symbol', 'datetime', name='uix_symbol_datetime'),
        Index('idx_symbol_datetime', 'symbol', 'datetime'),
    )
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "datetime": self.datetime.strftime("%Y-%m-%d %H:%M:%S") if self.datetime else None,
            "indicators": {
                "close": str(self.close) if self.close else "N/A",
                "vwap": str(self.vwap) if self.vwap else "N/A",
                "bbi": str(self.bbi) if self.bbi else "N/A",
                "bbiboll_upper": str(self.bbiboll_upper) if self.bbiboll_upper else "N/A",
                "bbiboll_lower": str(self.bbiboll_lower) if self.bbiboll_lower else "N/A",
                "bbiboll_ratio": str(self.bbiboll_ratio) if self.bbiboll_ratio else "N/A"
            }
        }


# ========== 新增：1 小时级 ==========
class IntradayHourlyBar(Base):
    """1 小时级 K 线 + 技术指标（当日）"""
    __tablename__ = 'intraday_hourly_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    datetime = Column(DateTime, nullable=False, index=True)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    
    vwap = Column(Float)
    bbi = Column(Float)
    bbiboll_upper = Column(Float)
    bbiboll_lower = Column(Float)
    bbiboll_ratio = Column(Float)
    
    bar_size = Column(String(10), default='1 hour')
    update_time = Column(DateTime)
    
    __table_args__ = (
        UniqueConstraint('symbol', 'datetime', name='uix_symbol_hourly'),
        Index('idx_symbol_hourly', 'symbol', 'datetime'),
    )
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "datetime": self.datetime.strftime("%Y-%m-%d %H:%M:%S") if self.datetime else None,
            "indicators": {
                "close": str(self.close) if self.close else "N/A",
                "vwap": str(self.vwap) if self.vwap else "N/A",
                "bbi": str(self.bbi) if self.bbi else "N/A",
                "bbiboll_upper": str(self.bbiboll_upper) if self.bbiboll_upper else "N/A",
                "bbiboll_lower": str(self.bbiboll_lower) if self.bbiboll_lower else "N/A",
                "bbiboll_ratio": str(self.bbiboll_ratio) if self.bbiboll_ratio else "N/A"
            }
        }


# ========== 新增：2 小时级 ==========
class Intraday2HourBar(Base):
    """2 小时级 K 线 + 技术指标（当日）"""
    __tablename__ = 'intraday_2hour_bars'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    datetime = Column(DateTime, nullable=False, index=True)
    
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    
    vwap = Column(Float)
    bbi = Column(Float)
    bbiboll_upper = Column(Float)
    bbiboll_lower = Column(Float)
    bbiboll_ratio = Column(Float)
    
    bar_size = Column(String(10), default='2 hour')
    update_time = Column(DateTime)
    
    __table_args__ = (
        UniqueConstraint('symbol', 'datetime', name='uix_symbol_2hour'),
        Index('idx_symbol_2hour', 'symbol', 'datetime'),
    )
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "datetime": self.datetime.strftime("%Y-%m-%d %H:%M:%S") if self.datetime else None,
            "indicators": {
                "close": str(self.close) if self.close else "N/A",
                "vwap": str(self.vwap) if self.vwap else "N/A",
                "bbi": str(self.bbi) if self.bbi else "N/A",
                "bbiboll_upper": str(self.bbiboll_upper) if self.bbiboll_upper else "N/A",
                "bbiboll_lower": str(self.bbiboll_lower) if self.bbiboll_lower else "N/A",
                "bbiboll_ratio": str(self.bbiboll_ratio) if self.bbiboll_ratio else "N/A"
            }
        }


# 创建所有表
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

def get_session():
    return Session()

def get_engine():
    return engine