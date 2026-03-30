#!/usr/bin/env python3
"""
交易日志与绩效记录模块
功能：
1. 记录每次 LLM 决策（prompt/output/tokens/耗时）
2. 记录每笔交易（symbol/action/qty/price/pnl）
3. 记录每日绩效（盈亏/胜率/最大回撤）
4. 支持 CSV + SQLite 双存储

设计原则：
- 决策日志与交易日志分离
- 支持盘后复盘查询
- 自动按日期分文件
"""
import os
import json
import csv
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd

from config import settings

logger = logging.getLogger(__name__)

# ========== 项目路径 ==========
PROJECT_ROOT = Path(__file__).parent.parent.parent
LOGS_DIR = PROJECT_ROOT / "data" / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# ========== 日志数据库 Schema ==========
Base = declarative_base()
DB_PATH = LOGS_DIR / "trading_logs.db"
engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
Session = sessionmaker(bind=engine)


class DecisionLog(Base):
    """LLM 决策日志表"""
    __tablename__ = 'decision_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String(10), index=True)  # YYYY-MM-DD
    time = Column(String(8), index=True)   # HH:MM:SS
    timestamp = Column(DateTime)
    
    symbol_pool = Column(Text)  # JSON 格式的资产池
    llm_model = Column(String(50))
    prompt_tokens = Column(Integer)
    completion_tokens = Column(Integer)
    total_tokens = Column(Integer)
    api_latency_sec = Column(Float)
    
    raw_output = Column(Text)  # LLM 原始输出
    parsed_action = Column(String(20))  # BUY/SELL/NO_OP
    parsed_symbol = Column(String(10))
    parsed_qty = Column(Integer)
    parsed_price = Column(Float)
    
    simulate_mode = Column(Boolean)
    risk_check_passed = Column(Boolean)
    executed = Column(Boolean)
    
    notes = Column(Text)  # 错误信息/备注
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "date": self.date,
            "time": self.time,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "llm_model": self.llm_model,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "api_latency_sec": self.api_latency_sec,
            "parsed_action": self.parsed_action,
            "parsed_symbol": self.parsed_symbol,
            "parsed_qty": self.parsed_qty,
            "parsed_price": self.parsed_price,
            "simulate_mode": self.simulate_mode,
            "risk_check_passed": self.risk_check_passed,
            "executed": self.executed,
            "notes": self.notes
        }


class TradeLog(Base):
    """交易执行日志表"""
    __tablename__ = 'trade_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String(10), index=True)
    time = Column(String(8), index=True)
    timestamp = Column(DateTime)
    
    symbol = Column(String(10), index=True)
    action = Column(String(10))  # BUY/SELL
    quantity = Column(Integer)
    price = Column(Float)
    order_id = Column(String(50))  # IBKR 订单 ID
    
    commission = Column(Float)
    pnl = Column(Float)  # 已实现盈亏
    pnl_percent = Column(Float)
    
    simulate_mode = Column(Boolean)
    status = Column(String(20))  # SUBMITTED/FILLED/CANCELLED/REJECTED
    
    decision_log_id = Column(Integer)  # 关联的决策日志 ID
    
    notes = Column(Text)
    
    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "date": self.date,
            "time": self.time,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "symbol": self.symbol,
            "action": self.action,
            "quantity": self.quantity,
            "price": self.price,
            "order_id": self.order_id,
            "commission": self.commission,
            "pnl": self.pnl,
            "pnl_percent": self.pnl_percent,
            "simulate_mode": self.simulate_mode,
            "status": self.status,
            "decision_log_id": self.decision_log_id,
            "notes": self.notes
        }


class DailyPerformance(Base):
    """每日绩效汇总表"""
    __tablename__ = 'daily_performance'
    
    date = Column(String(10), primary_key=True)
    
    total_trades = Column(Integer)
    winning_trades = Column(Integer)
    losing_trades = Column(Integer)
    win_rate = Column(Float)
    
    total_pnl = Column(Float)
    total_commission = Column(Float)
    net_pnl = Column(Float)
    
    max_drawdown = Column(Float)
    max_profit = Column(Float)
    
    total_tokens_used = Column(Integer)
    total_api_latency_sec = Column(Float)
    
    account_value_start = Column(Float)
    account_value_end = Column(Float)
    daily_return = Column(Float)
    
    notes = Column(Text)
    
    def to_dict(self) -> Dict:
        return {
            "date": self.date,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": self.win_rate,
            "total_pnl": self.total_pnl,
            "total_commission": self.total_commission,
            "net_pnl": self.net_pnl,
            "max_drawdown": self.max_drawdown,
            "max_profit": self.max_profit,
            "total_tokens_used": self.total_tokens_used,
            "total_api_latency_sec": self.total_api_latency_sec,
            "account_value_start": self.account_value_start,
            "account_value_end": self.account_value_end,
            "daily_return": self.daily_return,
            "notes": self.notes
        }


# 创建表
Base.metadata.create_all(engine)


class PerformanceLogger:
    """绩效日志管理器"""
    
    def __init__(self, date_str: str = None):
        self.date_str = date_str or datetime.now().strftime("%Y-%m-%d")
        self.session = Session()
        
        # CSV 文件路径（按日期分文件）
        self.decision_csv = LOGS_DIR / f"decisions_{self.date_str}.csv"
        self.trade_csv = LOGS_DIR / f"trades_{self.date_str}.csv"
        
        # 初始化 CSV 文件头
        self._init_csv_files()
        
        logger.info(f"📝 日志初始化完成：{self.date_str}")
    
    def _init_csv_files(self):
        """初始化 CSV 文件头"""
        # 决策日志 CSV
        if not self.decision_csv.exists():
            with open(self.decision_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "id", "date", "time", "llm_model", "prompt_tokens", 
                    "completion_tokens", "total_tokens", "api_latency_sec",
                    "parsed_action", "parsed_symbol", "parsed_qty", "parsed_price",
                    "simulate_mode", "risk_check_passed", "executed", "notes"
                ])
        
        # 交易日志 CSV
        if not self.trade_csv.exists():
            with open(self.trade_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "id", "date", "time", "symbol", "action", "quantity", "price",
                    "order_id", "commission", "pnl", "pnl_percent",
                    "simulate_mode", "status", "decision_log_id", "notes"
                ])
    
    def log_decision(self, decision_result: Dict[str, Any]) -> int:
        """
        记录一次 LLM 决策
        
        Args:
            decision_result: Agent.decide() 的返回值
                {
                    "raw_output": str,
                    "parsed": {"action": "BUY", "symbol": "AAPL", "qty": 100, "price": 150.0},
                    "usage": {"prompt_tokens": 1000, "completion_tokens": 50, "total_tokens": 1050},
                    "timestamp": "2026-03-28T09:35:00",
                    "simulate": True,
                    "model": "deepseek-chat",
                    "latency": 2.5,
                    "risk_check_passed": True,
                    "executed": True,
                    "error": None
                }
        
        Returns:
            决策日志 ID
        """
        now = datetime.now()
        timestamp = datetime.fromisoformat(decision_result.get("timestamp", now.isoformat()))
        parsed = decision_result.get("parsed") or {}
        usage = decision_result.get("usage") or {}
        
        log_entry = DecisionLog(
            date=self.date_str,
            time=now.strftime("%H:%M:%S"),
            timestamp=timestamp,
            symbol_pool="",  # 可选：记录资产池快照
            llm_model=decision_result.get("model", "unknown"),
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            api_latency_sec=decision_result.get("latency", 0),
            raw_output=decision_result.get("raw_output", "")[:10000],  # 限制长度
            parsed_action=parsed.get("action", "NO_OP"),
            parsed_symbol=parsed.get("symbol", ""),
            parsed_qty=parsed.get("qty", 0),
            parsed_price=parsed.get("price", 0),
            simulate_mode=decision_result.get("simulate", True),
            risk_check_passed=decision_result.get("risk_check_passed", False),
            executed=decision_result.get("executed", False),
            notes=decision_result.get("error", "")
        )
        
        self.session.add(log_entry)
        self.session.commit()
        
        # 同步写入 CSV
        self._append_decision_csv(log_entry)
        
        logger.info(f"📝 决策日志记录：ID={log_entry.id} | {parsed.get('action', 'NO_OP')}")
        return log_entry.id
    
    def _append_decision_csv(self, log_entry: DecisionLog):
        """追加决策日志到 CSV"""
        with open(self.decision_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                log_entry.id,
                log_entry.date,
                log_entry.time,
                log_entry.llm_model,
                log_entry.prompt_tokens,
                log_entry.completion_tokens,
                log_entry.total_tokens,
                f"{log_entry.api_latency_sec:.2f}",
                log_entry.parsed_action,
                log_entry.parsed_symbol,
                log_entry.parsed_qty,
                f"{log_entry.parsed_price:.2f}" if log_entry.parsed_price else "",
                log_entry.simulate_mode,
                log_entry.risk_check_passed,
                log_entry.executed,
                log_entry.notes[:500] if log_entry.notes else ""
            ])
    
    def log_trade(self, trade_info: Dict[str, Any], decision_log_id: int = None) -> int:
        """
        记录一笔交易
        
        Args:
            trade_info: 交易信息
                {
                    "symbol": "AAPL",
                    "action": "BUY",
                    "quantity": 100,
                    "price": 150.0,
                    "order_id": "123456789",
                    "commission": 1.0,
                    "pnl": 0.0,  # 买入时为 0
                    "pnl_percent": 0.0,
                    "status": "FILLED",
                    "simulate": True,
                    "notes": ""
                }
            decision_log_id: 关联的决策日志 ID
        
        Returns:
            交易日志 ID
        """
        now = datetime.now()
        
        log_entry = TradeLog(
            date=self.date_str,
            time=now.strftime("%H:%M:%S"),
            timestamp=now,
            symbol=trade_info.get("symbol", ""),
            action=trade_info.get("action", ""),
            quantity=trade_info.get("quantity", 0),
            price=trade_info.get("price", 0),
            order_id=trade_info.get("order_id", ""),
            commission=trade_info.get("commission", 0),
            pnl=trade_info.get("pnl", 0),
            pnl_percent=trade_info.get("pnl_percent", 0),
            simulate_mode=trade_info.get("simulate", True),
            status=trade_info.get("status", "SUBMITTED"),
            decision_log_id=decision_log_id,
            notes=trade_info.get("notes", "")
        )
        
        self.session.add(log_entry)
        self.session.commit()
        
        # 同步写入 CSV
        self._append_trade_csv(log_entry)
        
        logger.info(f"📝 交易日志记录：ID={log_entry.id} | {trade_info.get('action')} {trade_info.get('symbol')}")
        return log_entry.id
    
    def _append_trade_csv(self, log_entry: TradeLog):
        """追加交易日志到 CSV"""
        with open(self.trade_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                log_entry.id,
                log_entry.date,
                log_entry.time,
                log_entry.symbol,
                log_entry.action,
                log_entry.quantity,
                f"{log_entry.price:.2f}",
                log_entry.order_id,
                f"{log_entry.commission:.2f}",
                f"{log_entry.pnl:+.2f}",
                f"{log_entry.pnl_percent:+.2f}%",
                log_entry.simulate_mode,
                log_entry.status,
                log_entry.decision_log_id,
                log_entry.notes[:500] if log_entry.notes else ""
            ])
    
    def get_daily_decisions(self, date: str = None) -> List[Dict]:
        """获取指定日期的所有决策记录"""
        date = date or self.date_str
        logs = self.session.query(DecisionLog).filter_by(date=date).all()
        return [log.to_dict() for log in logs]
    
    def get_daily_trades(self, date: str = None) -> List[Dict]:
        """获取指定日期的所有交易记录"""
        date = date or self.date_str
        logs = self.session.query(TradeLog).filter_by(date=date).all()
        return [log.to_dict() for log in logs]
    
    def calculate_daily_performance(self, date: str = None) -> Dict:
        """
        计算指定日期的绩效汇总
        
        Returns:
            {
                "total_trades": 10,
                "winning_trades": 6,
                "losing_trades": 4,
                "win_rate": 0.6,
                "total_pnl": 500.0,
                "total_commission": 10.0,
                "net_pnl": 490.0,
                ...
            }
        """
        date = date or self.date_str
        trades = self.get_daily_trades(date)
        decisions = self.get_daily_decisions(date)
        
        if not trades:
            return {
                "date": date,
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "total_pnl": 0.0,
                "total_commission": 0.0,
                "net_pnl": 0.0,
                "total_tokens_used": sum(d.get("total_tokens", 0) for d in decisions),
                "notes": "No trades"
            }
        
        # 计算盈亏
        winning = [t for t in trades if t.get("pnl", 0) > 0]
        losing = [t for t in trades if t.get("pnl", 0) < 0]
        total_pnl = sum(t.get("pnl", 0) for t in trades)
        total_commission = sum(t.get("commission", 0) for t in trades)
        
        return {
            "date": date,
            "total_trades": len(trades),
            "winning_trades": len(winning),
            "losing_trades": len(losing),
            "win_rate": len(winning) / len(trades) if trades else 0,
            "total_pnl": total_pnl,
            "total_commission": total_commission,
            "net_pnl": total_pnl - total_commission,
            "max_profit": max((t.get("pnl", 0) for t in trades), default=0),
            "max_drawdown": min((t.get("pnl", 0) for t in trades), default=0),
            "total_tokens_used": sum(d.get("total_tokens", 0) for d in decisions),
            "total_api_latency_sec": sum(d.get("api_latency_sec", 0) for d in decisions)
        }
    
    def save_daily_performance(self, perf_data: Dict = None):
        """保存每日绩效汇总到数据库"""
        if perf_data is None:
            perf_data = self.calculate_daily_performance()
        
        existing = self.session.query(DailyPerformance).filter_by(date=perf_data["date"]).first()
        if existing:
            # 更新
            for key, value in perf_data.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
        else:
            # 插入
            new_entry = DailyPerformance(**perf_data)
            self.session.add(new_entry)
        
        self.session.commit()
        logger.info(f"📊 每日绩效已保存：{perf_data['date']}")
    
    def export_daily_report(self, date: str = None, output_path: str = None) -> str:
        """
        导出每日复盘报告（JSON 格式）
        
        Returns:
            报告文件路径
        """
        date = date or self.date_str
        if output_path is None:
            output_path = str(LOGS_DIR / f"report_{date}.json")
        
        report = {
            "date": date,
            "generated_at": datetime.now().isoformat(),
            "performance": self.calculate_daily_performance(date),
            "decisions": self.get_daily_decisions(date),
            "trades": self.get_daily_trades(date)
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📄 复盘报告已导出：{output_path}")
        return output_path
    
    def close(self):
        """关闭会话"""
        if self.session:
            self.session.close()
            logger.info("🔒 日志会话已关闭")


def create_logger(date_str: str = None) -> PerformanceLogger:
    """工厂函数：创建日志记录器"""
    return PerformanceLogger(date_str)