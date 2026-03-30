#!/usr/bin/env python3
"""
LLM 输出解析模块（支持市价单）
功能：
1. 解析 LLM 输出的 buy/sell 指令（限价/市价）
2. 支持多种格式（正则/JSON/自然语言）
3. 严格校验，失败时返回 NO_OP

支持的输出格式：
- 限价单：buy(AAPL, 100, 150.5) / sell(TSLA, 50, 200.0)
- 市价单：buy_mkt(AAPL, 100) / sell_mkt(TSLA, 50)
- JSON: {"action": "BUY", "symbol": "AAPL", "qty": 100, "price": 150.5, "order_type": "LMT/MKT"}
- NO_OP / 无操作 / hold
"""
import re
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class TradeInstructionParser:
    """交易指令解析器"""
    
    # 正则模式（限价单）
    LIMIT_PATTERNS = [
        # 模式 1: buy(symbol, qty, price)
        r'(?:buy|BUY)\s*\(\s*([A-Za-z]{1,5})\s*,\s*(\d+)\s*,\s*([\d.]+)\s*\)',
        # 模式 2: sell(symbol, qty, price)
        r'(?:sell|SELL)\s*\(\s*([A-Za-z]{1,5})\s*,\s*(\d+)\s*,\s*([\d.]+)\s*\)',
        # 模式 3: BUY AAPL 100 @ 150.5
        r'(?:BUY|Buy)\s+([A-Za-z]{1,5})\s+(\d+)\s*(?:@|at)\s*([\d.]+)',
        # 模式 4: SELL TSLA 50 @ 200.0
        r'(?:SELL|Sell)\s+([A-Za-z]{1,5})\s+(\d+)\s*(?:@|at)\s*([\d.]+)',
    ]
    
    # 正则模式（市价单）
    MARKET_PATTERNS = [
        # 模式 1: buy_mkt(symbol, qty)
        r'(?:buy_mkt|BUY_MKT|buy_mkt)\s*\(\s*([A-Za-z]{1,5})\s*,\s*(\d+)\s*\)',
        # 模式 2: sell_mkt(symbol, qty)
        r'(?:sell_mkt|SELL_MKT|sell_mkt)\s*\(\s*([A-Za-z]{1,5})\s*,\s*(\d+)\s*\)',
        # 模式 3: BUY MKT AAPL 100
        r'(?:BUY|Buy)\s+MKT\s+([A-Za-z]{1,5})\s+(\d+)',
        # 模式 4: SELL MKT TSLA 50
        r'(?:SELL|Sell)\s+MKT\s+([A-Za-z]{1,5})\s+(\d+)',
    ]
    
    # JSON 模式关键字
    JSON_ACTION_KEYS = ["action", "Action", "ACTION", "type", "Type"]
    JSON_SYMBOL_KEYS = ["symbol", "Symbol", "SYMBOL", "ticker", "Ticker"]
    JSON_QTY_KEYS = ["qty", "quantity", "Quantity", "QTY", "shares"]
    JSON_PRICE_KEYS = ["price", "Price", "PRICE", "limit_price", "limitPrice"]
    JSON_TYPE_KEYS = ["order_type", "orderType", "type", "Type"]
    
    # NO_OP 模式
    NO_OP_PATTERNS = [
        r'NO_OP',
        r'no\s*operation',
        r'无操作',
        r'hold',
        r'HOLD',
        r'保持观望',
        r'不交易',
        r'暂无操作',
        r'no\s*action',
        r'wait',
        r'观望',
    ]
    
    def __init__(self):
        self.limit_compiled = [re.compile(p, re.IGNORECASE) for p in self.LIMIT_PATTERNS]
        self.market_compiled = [re.compile(p, re.IGNORECASE) for p in self.MARKET_PATTERNS]
        self.noop_compiled = [re.compile(p, re.IGNORECASE) for p in self.NO_OP_PATTERNS]
    
    def parse(self, raw_output: str) -> Dict[str, Any]:
        """
        解析 LLM 原始输出
        
        Returns:
            {
                "action": "BUY" | "SELL" | "NO_OP",
                "symbol": Optional[str],
                "qty": Optional[int],
                "price": Optional[float],  # 限价单有值，市价单为 None
                "order_type": "LMT" | "MKT",
                "raw_output": str,
                "parse_method": "regex" | "json" | "noop",
                "confidence": float,
                "error": Optional[str]
            }
        """
        result = {
            "action": "NO_OP",
            "symbol": None,
            "qty": None,
            "price": None,
            "order_type": None,
            "raw_output": raw_output[:500],
            "parse_method": None,
            "confidence": 0.0,
            "error": None
        }
        
        if not raw_output or not raw_output.strip():
            result["parse_method"] = "empty"
            result["error"] = "Empty output"
            return result
        
        raw_output = raw_output.strip()
        
        # 1. 检查 NO_OP
        if self._is_noop(raw_output):
            result["action"] = "NO_OP"
            result["parse_method"] = "noop"
            result["confidence"] = 1.0
            logger.info("🔍 解析结果：NO_OP")
            return result
        
        # 2. 尝试 JSON 解析
        json_result = self._try_json_parse(raw_output)
        if json_result and json_result.get("action") in ("BUY", "SELL"):
            result.update(json_result)
            result["parse_method"] = "json"
            logger.info(f"🔍 解析结果：{result['order_type']} {result['action']} {result['symbol']} x{result['qty']}")
            return result
        
        # 3. 尝试市价单正则解析（优先）
        market_result = self._try_market_parse(raw_output)
        if market_result and market_result.get("action") in ("BUY", "SELL"):
            result.update(market_result)
            result["parse_method"] = "regex"
            logger.info(f"🔍 解析结果：MKT {result['action']} {result['symbol']} x{result['qty']}")
            return result
        
        # 4. 尝试限价单正则解析
        limit_result = self._try_limit_parse(raw_output)
        if limit_result and limit_result.get("action") in ("BUY", "SELL"):
            result.update(limit_result)
            result["parse_method"] = "regex"
            logger.info(f"🔍 解析结果：LMT {result['action']} {result['symbol']} x{result['qty']} @ {result['price']}")
            return result
        
        # 5. 解析失败
        result["parse_method"] = "failed"
        result["error"] = "Unable to parse any valid instruction"
        logger.warning(f"⚠️ 解析失败：{raw_output[:100]}...")
        return result
    
    def _is_noop(self, text: str) -> bool:
        """检查是否为无操作指令"""
        for pattern in self.noop_compiled:
            if pattern.search(text):
                return True
        return False
    
    def _try_json_parse(self, text: str) -> Optional[Dict]:
        """尝试 JSON 格式解析"""
        # 提取 JSON 块（支持代码块格式）
        json_match = re.search(r'```(?:json)?\s*({.*?})\s*```', text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = text
        
        try:
            data = json.loads(json_str)
            
            # 提取 action
            action = None
            for key in self.JSON_ACTION_KEYS:
                if key in data:
                    action = str(data[key]).upper()
                    break
            
            if action not in ("BUY", "SELL"):
                return None
            
            # 提取 symbol
            symbol = None
            for key in self.JSON_SYMBOL_KEYS:
                if key in data:
                    symbol = str(data[key]).upper()
                    break
            
            # 提取 qty
            qty = None
            for key in self.JSON_QTY_KEYS:
                if key in data:
                    qty = int(data[key])
                    break
            
            # 提取 order_type
            order_type = "LMT"  # 默认限价
            for key in self.JSON_TYPE_KEYS:
                if key in data:
                    ot = str(data[key]).upper()
                    order_type = "MKT" if ot in ("MKT", "MARKET", "市价") else "LMT"
                    break
            
            # 提取 price（限价单必需）
            price = None
            for key in self.JSON_PRICE_KEYS:
                if key in data:
                    price = float(data[key])
                    break
            
            # 校验：限价单必须有价格
            if order_type == "LMT" and not price:
                return None
            
            if not symbol or not qty:
                return None
            
            return {
                "action": action,
                "symbol": symbol,
                "qty": qty,
                "price": price,
                "order_type": order_type,
                "confidence": 0.9
            }
        except (json.JSONDecodeError, ValueError, TypeError):
            return None
    
    def _try_limit_parse(self, text: str) -> Optional[Dict]:
        """尝试限价单正则解析"""
        for i, pattern in enumerate(self.limit_compiled):
            match = pattern.search(text)
            if match:
                groups = match.groups()
                if len(groups) >= 3:
                    symbol = groups[0].upper()
                    qty = int(groups[1])
                    price = float(groups[2])
                    
                    # 确定动作类型
                    action = "BUY" if i % 2 == 0 else "SELL"
                    
                    # 基本校验
                    if not symbol.isalnum() or len(symbol) > 5:
                        continue
                    if qty <= 0 or price <= 0:
                        continue
                    
                    return {
                        "action": action,
                        "symbol": symbol,
                        "qty": qty,
                        "price": price,
                        "order_type": "LMT",
                        "confidence": 0.8
                    }
        return None
    
    def _try_market_parse(self, text: str) -> Optional[Dict]:
        """尝试市价单正则解析"""
        for i, pattern in enumerate(self.market_compiled):
            match = pattern.search(text)
            if match:
                groups = match.groups()
                if len(groups) >= 2:
                    symbol = groups[0].upper()
                    qty = int(groups[1])
                    
                    # 确定动作类型
                    action = "BUY" if i % 2 == 0 else "SELL"
                    
                    # 基本校验
                    if not symbol.isalnum() or len(symbol) > 5:
                        continue
                    if qty <= 0:
                        continue
                    
                    return {
                        "action": action,
                        "symbol": symbol,
                        "qty": qty,
                        "price": None,  # 市价单无价格
                        "order_type": "MKT",
                        "confidence": 0.8
                    }
        return None
    
    def validate_instruction(self, parsed: Dict[str, Any], 
                           positions: Dict, cash: float,
                           current_prices: Dict[str, float],
                           max_position_pct: float = 0.25) -> Dict[str, Any]:
        """
        校验交易指令的合理性
        
        Args:
            parsed: 解析后的指令
            positions: 当前持仓 {symbol: {shares, avg_cost, ...}}
            cash: 可用现金
            current_prices: 当前价格 {symbol: price}
            max_position_pct: 单股票最大仓位比例
        
        Returns:
            {
                "valid": bool,
                "errors": List[str],
                "warnings": List[str]
            }
        """
        errors = []
        warnings = []
        
        if parsed["action"] == "NO_OP":
            return {"valid": True, "errors": [], "warnings": []}
        
        symbol = parsed["symbol"]
        qty = parsed["qty"]
        price = parsed["price"]
        order_type = parsed.get("order_type", "LMT")
        
        # 获取当前价格（市价单估算用）
        current_price = current_prices.get(symbol, price)
        estimated_value = current_price * qty if current_price else 0
        
        # 1. 符号校验
        if not symbol or not symbol.isalnum() or len(symbol) > 5:
            errors.append(f"Invalid symbol: {symbol}")
        
        # 2. 数量校验
        if qty <= 0:
            errors.append(f"Invalid quantity: {qty}")
        elif qty % 100 != 0 and qty < 100:
            warnings.append(f"Quantity {qty} is not a round lot (100 shares)")
        
        # 3. 价格校验（仅限价单）
        if order_type == "LMT":
            if not price or price <= 0:
                errors.append(f"Invalid price for limit order: {price}")
            elif price > 10000:
                warnings.append(f"High price alert: ${price}")
        
        # 4. 资金校验（买入）
        if parsed["action"] == "BUY":
            if order_type == "LMT":
                required_cash = estimated_value
            else:  # MKT - 增加 2% 滑点缓冲
                required_cash = estimated_value * 1.02
            
            if required_cash > cash:
                errors.append(f"Insufficient cash: need ${required_cash:.2f}, have ${cash:.2f}")
        
        # 5. 持仓校验（卖出）
        if parsed["action"] == "SELL":
            current_shares = positions.get(symbol, {}).get("shares", 0)
            if qty > current_shares:
                errors.append(f"Insufficient shares: have {current_shares}, selling {qty}")
        
        # 6. 仓位集中度校验（买入）
        if parsed["action"] == "BUY":
            current_value = positions.get(symbol, {}).get("shares", 0) * positions.get(symbol, {}).get("current_price", 0)
            new_value = current_value + estimated_value
            estimated_total = cash + sum(p.get("shares", 0) * p.get("current_price", 0) for p in positions.values())
            if estimated_total > 0 and new_value / estimated_total > max_position_pct:
                warnings.append(f"Position concentration: {new_value/estimated_total*100:.1f}% > {max_position_pct*100}%")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }


def create_parser() -> TradeInstructionParser:
    """工厂函数：创建解析器"""
    return TradeInstructionParser()