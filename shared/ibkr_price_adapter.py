#!/usr/bin/env python3
"""
IBKR 价格数据适配器 - 四层数据库路由
支持指标：close, vwap, bbi, bbiboll_upper, bbiboll_lower, bbiboll_ratio
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text
from ib_insync import IB, Stock, util
import pandas as pd

logger = logging.getLogger(__name__)

# ================= 配置 =================
IB_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IBKR_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", "22"))

HISTORICAL_DB_PATH = os.getenv("IBKR_DB_PATH", "ibkr_us_stocks.db")
INTRADAY_DB_PATH = os.getenv("INTRADAY_DB_PATH", "ibkr_intraday.db")

# ================= 适配器 =================
class IBKRPriceAdapter:
    _instance = None
    _ib = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._ib = None
            self._historical_engine = create_engine(f'sqlite:///{HISTORICAL_DB_PATH}')
            self._intraday_engine = create_engine(f'sqlite:///{INTRADAY_DB_PATH}')
    
    def _get_ib_connection(self) -> Optional[IB]:
        if self._ib is None or not self._ib.isConnected():
            try:
                self._ib = IB()
                self._ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)
                logger.info(f"✅ IBKR 连接成功")
            except Exception as e:
                logger.error(f"❌ IBKR 连接失败：{e}")
                return None
        return self._ib
    
    def _is_today(self, date_str: str) -> bool:
        """判断是否是今天"""
        if not date_str:
            return True
        try:
            target = datetime.strptime(date_str.split()[0], "%Y-%m-%d")
            return target.date() == datetime.now().date()
        except:
            return True
    
    def get_from_intraday_db(self, symbol: str) -> Optional[Dict[str, Any]]:
        """从 intraday DB 获取最新 5 分钟 K 线 + 指标"""
        try:
            with self._intraday_engine.connect() as conn:
                query = text("""
                    SELECT symbol, datetime, open, high, low, close, volume, 
                           vwap, bbi, bbiboll_upper, bbiboll_lower, bbiboll_ratio
                    FROM intraday_bars
                    WHERE symbol = :symbol
                    ORDER BY datetime DESC
                    LIMIT 1
                """)
                result = conn.execute(query, {"symbol": symbol}).fetchone()
                
                if result:
                    logger.info(f"📊 [INTRADAY] {symbol} 数据来自 intraday DB")
                    return {
                        "symbol": result[0],
                        "date": str(result[1]),
                        "ohlcv": {
                            "open": str(result[2]) if result[2] else "0",
                            "high": str(result[3]) if result[3] else "0",
                            "low": str(result[4]) if result[4] else "0",
                            "close": str(result[5]) if result[5] else "0",
                            "volume": str(result[6]) if result[6] else "0"
                        },
                        "indicators": {
                            "close": str(result[5]) if result[5] else "N/A",
                            "vwap": str(result[7]) if result[7] else "N/A",
                            "bbi": str(result[8]) if result[8] else "N/A",
                            "bbiboll_upper": str(result[9]) if result[9] else "N/A",
                            "bbiboll_lower": str(result[10]) if result[10] else "N/A",
                            "bbiboll_ratio": str(result[11]) if result[11] else "N/A"
                        }
                    }
        except Exception as e:
            logger.error(f"❌ [INTRADAY] 查询失败：{e}")
        return None
    
    def get_from_historical_db(self, symbol: str, date: str = None) -> Optional[Dict[str, Any]]:
        """从历史 DB 获取日线数据"""
        try:
            with self._historical_engine.connect() as conn:
                if date:
                    query = text("""
                        SELECT symbol, date, open, high, low, close, volume
                        FROM daily_bars
                        WHERE symbol = :symbol AND date LIKE :date
                        ORDER BY date DESC
                        LIMIT 1
                    """)
                    result = conn.execute(query, {
                        "symbol": symbol, 
                        "date": f"{date}%"
                    }).fetchone()
                else:
                    query = text("""
                        SELECT symbol, date, open, high, low, close, volume
                        FROM daily_bars
                        WHERE symbol = :symbol
                        ORDER BY date DESC
                        LIMIT 1
                    """)
                    result = conn.execute(query, {"symbol": symbol}).fetchone()
                
                if result:
                    logger.info(f"📊 [HISTORICAL] {symbol} 数据来自 historical DB")
                    return {
                        "symbol": result[0],
                        "date": str(result[1]),
                        "ohlcv": {
                            "open": str(result[2]),
                            "high": str(result[3]),
                            "low": str(result[4]),
                            "close": str(result[5]),
                            "volume": str(result[6])
                        },
                        "indicators": {
                            "close": str(result[5]),
                            "vwap": "N/A",
                            "bbi": "N/A",
                            "bbiboll_upper": "N/A",
                            "bbiboll_lower": "N/A",
                            "bbiboll_ratio": "N/A"
                        }
                    }
        except Exception as e:
            logger.error(f"❌ [HISTORICAL] 查询失败：{e}")
        return None
    
    def get_from_ibkr_api(self, symbol: str) -> Optional[Dict[str, Any]]:
        """从 IBKR API 直接获取（降级方案）"""
        ib = self._get_ib_connection()
        if not ib:
            return None
        
        try:
            contract = Stock(symbol, "SMART", "USD")
            bars = ib.reqHistoricalData(
                contract, endDateTime='', durationStr='1 D',
                barSizeSetting='5 mins', whatToShow='TRADES',
                useRTH=True, formatDate=1
            )
            
            if not bars:
                return None
            
            latest = bars[-1]
            date_str = latest.date.strftime("%Y-%m-%d %H:%M:%S") if hasattr(latest.date, 'strftime') else str(latest.date)
            
            logger.info(f"📊 [IBKR API] {symbol} 数据来自 IBKR API")
            
            return {
                "symbol": symbol,
                "date": date_str,
                "ohlcv": {
                    "open": str(latest.open) if latest.open else "0",
                    "high": str(latest.high) if latest.high else "0",
                    "low": str(latest.low) if latest.low else "0",
                    "close": str(latest.close) if latest.close else "0",
                    "volume": str(int(latest.volume)) if latest.volume else "0"
                },
                "indicators": {
                    "close": str(latest.close) if latest.close else "N/A",
                    "vwap": "N/A",
                    "bbi": "N/A",
                    "bbiboll_upper": "N/A",
                    "bbiboll_lower": "N/A",
                    "bbiboll_ratio": "N/A"
                }
            }
        except Exception as e:
            logger.error(f"❌ [IBKR API] 获取失败：{e}")
            return None
    
    def get_price(self, symbol: str, date: str = None) -> Optional[Dict[str, Any]]:
        """
        统一接口：四层路由
        1. 今天 → intraday DB
        2. 历史日期 → historical DB
        3. 都失败 → IBKR API
        """
        logger.info(f"🔍 请求价格：{symbol}, date={date}")
        
        if self._is_today(date):
            data = self.get_from_intraday_db(symbol)
            if data:
                return data
        
        data = self.get_from_historical_db(symbol, date)
        if data:
            return data
        
        logger.info(f"🔄 DB 无数据，切换到 IBKR API")
        data = self.get_from_ibkr_api(symbol)
        if data:
            return data
        
        logger.error(f"❌ 所有数据源失败：{symbol}")
        return None
    
    def disconnect(self):
        if self._ib and self._ib.isConnected():
            self._ib.disconnect()

# 全局单例
_adapter = None

def get_adapter() -> IBKRPriceAdapter:
    global _adapter
    if _adapter is None:
        _adapter = IBKRPriceAdapter()
    return _adapter