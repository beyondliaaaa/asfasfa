#!/usr/bin/env python3
"""
风控校验模块
功能：
1. 资金充足性检查
2. 持仓上限检查
3. 交易频率检查
4. 实盘开关检查
5. 黑名单/白名单检查

设计原则：
- 所有检查通过才允许执行
- 详细记录失败原因
- 支持模拟模式绕过部分检查
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pathlib import Path

from config import settings
from src.monitor.performance_logger import PerformanceLogger

logger = logging.getLogger(__name__)


class RiskChecker:
    """风控校验器"""
    
    def __init__(self, date_str: str = None):
        self.date_str = date_str or datetime.now().strftime("%Y-%m-%d")
        self.logger = PerformanceLogger(self.date_str)
        
        # 风控参数
        self.max_position_pct = settings.MAX_POSITION_PER_STOCK
        self.max_daily_trades = settings.MAX_DAILY_TRADES
        self.min_cash_reserve = settings.MIN_CASH_RESERVE
        self.enable_real_trading = settings.ENABLE_REAL_TRADING
        
        # 今日交易计数（初始化时查询）
        self.today_trade_count = self._get_today_trade_count()
    
    def check_real_trading_enabled(self) -> Dict[str, Any]:
        """检查实盘开关"""
        if not self.enable_real_trading:
            return {
                "passed": True,
                "mode": "SIMULATED",
                "message": "实盘模式未启用，将在模拟模式下执行"
            }
        return {
            "passed": True,
            "mode": "LIVE",
            "message": "实盘模式已启用"
        }
    
    def _get_today_trade_count(self) -> int:
        """获取今日已执行交易数"""
        today_trades = self.logger.get_daily_trades(self.date_str)
        executed_trades = [t for t in today_trades if t.get("status") in ("FILLED", "SIMULATED")]
        return len(executed_trades)

    def check_daily_trade_limit(self) -> Dict[str, Any]:
        """检查单日交易次数限制"""
        # 使用初始化时查询的计数
        if self.today_trade_count >= self.max_daily_trades:
            return {
                "passed": False,
                "reason": f"达到单日交易上限：{self.today_trade_count}/{self.max_daily_trades}",
                "current_count": self.today_trade_count,
                "max_count": self.max_daily_trades
            }
        
        return {
            "passed": True,
            "current_count": self.today_trade_count,
            "max_count": self.max_daily_trades,
            "remaining": self.max_daily_trades - self.today_trade_count
        }
    
    def check_cash_availability(self, action: str, estimated_value: float, 
                                total_account_value: float, available_cash: float) -> Dict[str, Any]:
        """
        检查资金充足性
        
        Args:
            action: BUY/SELL
            estimated_value: 预估交易金额
            total_account_value: 账户总值
            available_cash: 可用现金
        """
        if action == "SELL":
            return {"passed": True, "message": "卖出操作无需资金检查"}
        
        # 检查最低现金保留
        min_cash_required = total_account_value * self.min_cash_reserve
        if available_cash - estimated_value < min_cash_required:
            return {
                "passed": False,
                "reason": f"交易后现金低于保留要求：剩余 ${available_cash - estimated_value:.2f} < ${min_cash_required:.2f}",
                "available_cash": available_cash,
                "estimated_value": estimated_value,
                "min_required": min_cash_required
            }
        
        # 检查资金充足
        if estimated_value > available_cash:
            return {
                "passed": False,
                "reason": f"资金不足：需要 ${estimated_value:.2f}, 可用 ${available_cash:.2f}",
                "available_cash": available_cash,
                "estimated_value": estimated_value
            }
        
        return {
            "passed": True,
            "available_cash": available_cash,
            "estimated_value": estimated_value,
            "remaining_cash": available_cash - estimated_value
        }
    
    def check_position_limit(self, symbol: str, action: str, qty: int, 
                            current_price: float, positions: Dict, 
                            total_account_value: float) -> Dict[str, Any]:
        """
        检查持仓集中度限制
        
        Args:
            symbol: 股票代码
            action: BUY/SELL
            qty: 数量
            current_price: 当前价格
            positions: 当前持仓
            total_account_value: 账户总值
        """
        if action == "SELL":
            return {"passed": True, "message": "卖出操作无需持仓限制检查"}
        
        # 计算当前持仓价值
        current_shares = positions.get(symbol, {}).get("shares", 0)
        current_value = current_shares * positions.get(symbol, {}).get("current_price", current_price)
        
        # 计算新持仓价值
        new_value = current_value + (qty * current_price)
        
        # 计算仓位比例
        if total_account_value <= 0:
            return {"passed": False, "reason": "账户总值无效"}
        
        position_pct = new_value / total_account_value
        
        if position_pct > self.max_position_pct:
            return {
                "passed": False,
                "reason": f"单股票仓位超限：{position_pct*100:.1f}% > {self.max_position_pct*100:.1f}%",
                "symbol": symbol,
                "current_shares": current_shares,
                "new_shares": current_shares + qty,
                "position_pct": position_pct,
                "max_pct": self.max_position_pct
            }
        
        return {
            "passed": True,
            "symbol": symbol,
            "current_shares": current_shares,
            "new_shares": current_shares + qty,
            "position_pct": position_pct,
            "max_pct": self.max_position_pct
        }
    
    def check_symbol_allowed(self, symbol: str, allowed_symbols: List[str]) -> Dict[str, Any]:
        """检查股票是否在允许列表中"""
        if not allowed_symbols:
            return {"passed": True, "message": "无限制列表"}
        
        if symbol.upper() not in [s.upper() for s in allowed_symbols]:
            return {
                "passed": False,
                "reason": f"股票不在允许列表中：{symbol}",
                "symbol": symbol,
                "allowed": allowed_symbols
            }
        
        return {"passed": True, "symbol": symbol}
    
    def full_check(self, parsed_instruction: Dict, account_info: Dict, 
                   allowed_symbols: List[str] = None) -> Dict[str, Any]:
        """
        完整风控检查流程
        
        Args:
            parsed_instruction: 解析后的交易指令
                {
                    "action": "BUY/SELL/NO_OP",
                    "symbol": "AAPL",
                    "qty": 100,
                    "price": 150.0,  # 限价单有值，市价单为 None
                    "order_type": "LMT/MKT"
                }
            account_info: 账户信息
                {
                    "total_value": 100000.0,
                    "available_cash": 50000.0,
                    "positions": {
                        "AAPL": {"shares": 100, "current_price": 150.0},
                        ...
                    },
                    "current_prices": {
                        "AAPL": 150.0,
                        ...
                    }
                }
            allowed_symbols: 允许交易的股票列表（资产池）
        
        Returns:
            {
                "passed": bool,
                "mode": "SIMULATED/LIVE",
                "checks": {
                    "real_trading": {...},
                    "daily_limit": {...},
                    "cash": {...},
                    "position": {...},
                    "symbol": {...}
                },
                "errors": List[str],
                "warnings": List[str]
            }
        """
        result = {
            "passed": True,
            "mode": "SIMULATED",
            "checks": {},
            "errors": [],
            "warnings": []
        }
        
        # NO_OP 直接通过
        if parsed_instruction.get("action") == "NO_OP":
            result["message"] = "无操作指令，跳过风控检查"
            return result
        
        # 1. 实盘开关检查
        real_check = self.check_real_trading_enabled()
        result["checks"]["real_trading"] = real_check
        result["mode"] = real_check["mode"]
        
        # 2. 单日交易次数检查
        daily_check = self.check_daily_trade_limit()
        result["checks"]["daily_limit"] = daily_check
        if not daily_check["passed"]:
            result["passed"] = False
            result["errors"].append(daily_check["reason"])
        
        # 3. 股票允许列表检查
        symbol = parsed_instruction.get("symbol")
        if symbol:
            symbol_check = self.check_symbol_allowed(symbol, allowed_symbols)
            result["checks"]["symbol"] = symbol_check
            if not symbol_check["passed"]:
                result["passed"] = False
                result["errors"].append(symbol_check["reason"])
        
        # 4. 资金检查（买入）
        action = parsed_instruction.get("action")
        qty = parsed_instruction.get("qty", 0)
        price = parsed_instruction.get("price")
        order_type = parsed_instruction.get("order_type", "LMT")
        
        # 获取当前价格（市价单用）
        current_price = account_info.get("current_prices", {}).get(symbol, price)
        if order_type == "MKT":
            # 市价单增加 2% 滑点缓冲
            estimated_value = current_price * qty * 1.02
        else:
            estimated_value = price * qty if price else current_price * qty
        
        cash_check = self.check_cash_availability(
            action=action,
            estimated_value=estimated_value,
            total_account_value=account_info.get("total_value", 0),
            available_cash=account_info.get("available_cash", 0)
        )
        result["checks"]["cash"] = cash_check
        if not cash_check["passed"]:
            result["passed"] = False
            result["errors"].append(cash_check["reason"])
        
        # 5. 持仓限制检查（买入）
        position_check = self.check_position_limit(
            symbol=symbol,
            action=action,
            qty=qty,
            current_price=current_price,
            positions=account_info.get("positions", {}),
            total_account_value=account_info.get("total_value", 0)
        )
        result["checks"]["position"] = position_check
        if not position_check["passed"]:
            result["passed"] = False
            result["errors"].append(position_check["reason"])
        
        # 汇总警告
        for check in result["checks"].values():
            if isinstance(check, dict) and "warnings" in check:
                result["warnings"].extend(check.get("warnings", []))
        
        # 记录日志
        if result["passed"]:
            logger.info(f"✅ 风控检查通过：{action} {symbol} x{qty} [{result['mode']}]")
        else:
            logger.warning(f"❌ 风控检查失败：{action} {symbol} x{qty} | 错误：{result['errors']}")
        
        return result


def create_risk_checker(date_str: str = None) -> RiskChecker:
    """工厂函数：创建风控检查器"""
    return RiskChecker(date_str)