#!/usr/bin/env python3
"""
IBKR 交易执行模块
功能：
1. 复用 ibkr_price_adapter 的 IBKR 连接
2. 执行限价单/市价单
3. 订单状态追踪
4. 模拟模式支持

设计原则：
- 执行前必须通过风控检查
- 所有订单记录到日志
- 支持模拟模式（记录但不执行）
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from ib_insync import IB, Stock, MarketOrder, LimitOrder, OrderStatus

from config import settings
from shared.ibkr_price_adapter import IBKRPriceAdapter
from src.monitor.performance_logger import PerformanceLogger

logger = logging.getLogger(__name__)


class IBKRExecutor:
    """IBKR 交易执行器"""
    
    def __init__(self, simulate: bool = None):
        """
        Args:
            simulate: 是否模拟模式（None 时读取配置）
        """
        self.simulate = simulate if simulate is not None else not settings.ENABLE_REAL_TRADING
        self.adapter = IBKRPriceAdapter()
        self.ib = None
        self.logger = PerformanceLogger()
        self.order_cache = {}  # 订单 ID → 订单信息
        
        if not self.simulate:
            self._connect_ibkr()
    
    def _connect_ibkr(self):
        """连接 IBKR"""
        if self.ib and self.ib.isConnected():
            logger.info("✅ IBKR 已连接")
            return
        
        try:
            self.ib = IB()
            self.ib.connect(
                host=settings.IB_HOST,
                port=settings.IB_PORT,
                clientId=settings.IB_CLIENT_ID,
                timeout=10
            )
            logger.info(f"✅ IBKR 连接成功：{settings.IB_HOST}:{settings.IB_PORT}")
        except Exception as e:
            logger.error(f"❌ IBKR 连接失败：{e}")
            self.simulate = True  # 连接失败自动切换模拟
            logger.warning("⚠️ 自动切换到模拟模式")
    
    def _create_order(self, action: str, qty: int, price: float = None, 
                      order_type: str = "LMT") -> Any:
        """
        创建 IBKR 订单对象
        
        Args:
            action: BUY/SELL
            qty: 数量
            price: 价格（限价单必需）
            order_type: LMT/MKT
        """
        if order_type == "MKT":
            return MarketOrder(action=action, totalQuantity=qty)
        else:
            return LimitOrder(action=action, totalQuantity=qty, lmtPrice=price)
    
    def submit_order(self, symbol: str, action: str, qty: int, 
                     price: float = None, order_type: str = "LMT") -> Dict[str, Any]:
        """
        提交订单
        
        Args:
            symbol: 股票代码
            action: BUY/SELL
            qty: 数量
            price: 价格（限价单必需）
            order_type: LMT/MKT
        
        Returns:
            {
                "success": bool,
                "order_id": Optional[str],
                "status": str,  # SUBMITTED/FILLED/CANCELLED/REJECTED/SIMULATED
                "message": str,
                "fill_price": Optional[float],
                "fill_qty": Optional[int],
                "commission": Optional[float]
            }
        """
        result = {
            "success": False,
            "order_id": None,
            "status": "PENDING",
            "message": "",
            "fill_price": None,
            "fill_qty": None,
            "commission": 0.0,
            "timestamp": datetime.now().isoformat()
        }
        
        # 模拟模式
        if self.simulate:
            logger.info(f"🎭 [SIMULATED] {action} {qty} {symbol} @ ${price if price else 'MKT'}")
            result["success"] = True
            result["order_id"] = f"SIM_{datetime.now().strftime('%Y%m%d%H%M%S')}_{symbol}"
            result["status"] = "SIMULATED"
            result["message"] = "模拟模式：订单已记录但未执行"
            result["fill_price"] = price
            result["fill_qty"] = qty
            return result
        
        # 实盘模式
        try:
            if not self.ib or not self.ib.isConnected():
                self._connect_ibkr()
                if not self.ib:
                    result["message"] = "IBKR 连接失败"
                    result["status"] = "REJECTED"
                    return result
            
            # 创建合约
            contract = Stock(symbol, "SMART", "USD")
            
            # 创建订单
            order = self._create_order(action, qty, price, order_type)
            
            # 提交订单
            trade = self.ib.placeOrder(contract, order)
            
            # 等待订单状态（可选：异步处理）
            self.ib.waitOnUpdate(timeout=5)
            
            order_id = str(trade.order.orderId)
            result["success"] = True
            result["order_id"] = order_id
            result["status"] = trade.orderStatus.status
            
            # 记录订单缓存
            self.order_cache[order_id] = {
                "symbol": symbol,
                "action": action,
                "qty": qty,
                "price": price,
                "order_type": order_type,
                "submitted_at": datetime.now()
            }
            
            logger.info(f"✅ 订单提交：{order_id} | {action} {qty} {symbol} @ ${price if price else 'MKT'}")
            
        except Exception as e:
            logger.error(f"❌ 订单提交失败：{e}")
            result["message"] = str(e)
            result["status"] = "REJECTED"
        
        return result
    
    def check_order_status(self, order_id: str) -> Dict[str, Any]:
        """检查订单状态"""
        if order_id.startswith("SIM_"):
            return {
                "order_id": order_id,
                "status": "FILLED",  # 模拟单直接视为成交
                "fill_price": None,
                "fill_qty": None
            }
        
        if not self.ib or not self.ib.isConnected():
            return {"order_id": order_id, "status": "UNKNOWN", "message": "IBKR 未连接"}
        
        # 实际实现需要查询 IBKR API
        # 这里简化处理
        return {
            "order_id": order_id,
            "status": "UNKNOWN",
            "message": "需要实现订单状态查询"
        }
    
    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """取消订单"""
        if order_id.startswith("SIM_"):
            return {
                "success": True,
                "message": "模拟订单已取消"
            }
        
        if not self.ib or not self.ib.isConnected():
            return {"success": False, "message": "IBKR 未连接"}
        
        # 实际实现需要调用 IBKR API
        return {"success": False, "message": "需要实现取消订单"}
    
    def get_account_info(self) -> Dict[str, Any]:
        """获取账户信息"""
        if self.simulate:
            return {
                "total_value": 100000.0,  # 模拟值
                "available_cash": 50000.0,
                "positions": {}
            }
        
        if not self.ib or not self.ib.isConnected():
            self._connect_ibkr()
        
        # 获取账户信息
        accounts = self.ib.managedAccounts()
        if not accounts:
            return {"total_value": 0, "available_cash": 0, "positions": {}}
        
        # 简化实现，实际需要从 IBKR 获取
        return {
            "total_value": 0,
            "available_cash": 0,
            "positions": {}
        }
    
    def disconnect(self):
        """断开连接"""
        if self.ib and self.ib.isConnected():
            self.ib.disconnect()
            logger.info("🔌 IBKR 已断开连接")
        if self.adapter:
            self.adapter.disconnect()


def create_executor(simulate: bool = None) -> IBKRExecutor:
    """工厂函数：创建执行器"""
    return IBKRExecutor(simulate=simulate)