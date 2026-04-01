#!/usr/bin/env python3
"""
AI-Trader-Intraday 统一入口
支持三种运行模式：
- premarket: 盘前准备（9:00 AM EST）
- intraday: 盘中决策循环（每 5 分钟，9:35 AM - 4:00 PM）
- review: 盘后复盘（4:30 PM EST）

用法：
    python main.py --mode=premarket --pool=config/stock_pool_template.txt
    python main.py --mode=intraday --pool=config/stock_pool_template.txt
    python main.py --mode=review --date=20260328
"""
import os
import sys
import argparse
import logging
import signal
import time
import pytz
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from src.monitor.performance_logger import PerformanceLogger

# ✅ 直接导入所有需要的模块（修复 NameError）
from src.data_prep import build_daily_pool
from src.agent import prompts, intraday_agent, parser
from src.execution import risk_checker, ibkr_executor
from src.data_prep import update_intraday

logger = logging.getLogger(__name__)

# 全局状态
running = True
current_mode = None


def signal_handler(signum, frame):
    """优雅退出处理"""
    global running
    logger.info(f"🚨 收到信号 {signum}，准备退出...")
    running = False


def setup_logging(mode: str, date_str: str):
    """配置日志"""
    log_file = settings.LOGS_DIR / f"trader_{date_str}_{mode}.log"
    
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger.info(f"📝 日志文件：{log_file}")
    logger.info(f"🚀 AI-Trader-Intraday 启动 | 模式：{mode} | 日期：{date_str}")

def save_prompt_log(prompt: str, cycle: int, date_str: str, keep_last_n: int = 5):
    """
    保存 Prompt 到日志文件
    同时清理旧文件，只保留最近 N 个
    """
    from pathlib import Path
    from config import settings
    
    if not getattr(settings, 'SAVE_PROMPT_LOGS', True):
        return None
    
    if keep_last_n is None:
        keep_last_n = getattr(settings, 'PROMPT_LOG_KEEP_LAST', 5)
        
    prompt_dir = PROJECT_ROOT / "data" / "prompts"
    prompt_dir.mkdir(exist_ok=True)
    
    # 按日期和循环编号保存
    prompt_file = prompt_dir / f"prompt_{date_str.replace('-', '')}_premarket.txt"
    
    try:
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(f"# AI-Trader-Intraday Prompt Log\n")
            f.write(f"# Mode: premarket\n")
            f.write(f"# Date: {date_str}\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write(f"# Length: {len(prompt)} chars (~{len(prompt)//4} tokens)\n")
            f.write(f"{'='*70}\n\n")
            f.write(prompt)
        
        logger.info(f"📝 Prompt 已保存：{prompt_file}")
        return str(prompt_file)
    except Exception as e:
        logger.error(f"❌ 保存 Prompt 失败：{e}")
        return None
    
# ================= 盘前准备模式 =================
def run_premarket(pool_path: str):
    """盘前准备流程"""
    logger.info("=" * 70)
    logger.info("🌅 盘前准备模式")
    logger.info("=" * 70)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    perf_logger = PerformanceLogger(date_str)
    
    try:
        # 1. 解析资产池
        logger.info(f"📋 加载资产池：{pool_path}")
        pool_config = build_daily_pool.parse_stock_pool_template(pool_path)
        if not pool_config:
            logger.error("❌ 资产池为空")
            return False
        
        logger.info(f"✅ 资产池加载完成：{len(pool_config)} 只股票")
        
        # ✅ 新增：验证股票是否在历史数据库中
        from src.data_prep.validate_pool import validate_stock_pool
        valid_pool, missing_symbols = validate_stock_pool(pool_config)
        
        if not valid_pool:
            logger.error("❌ 有效股票池为空，无法继续")
            return False
        
        if missing_symbols:
            logger.warning(f"⚠️ {len(missing_symbols)} 只股票将被跳过，详见警告日志")
        
        pool_config = valid_pool  # 使用验证后的资产池
        
        # 2. 初始化适配器
        adapter = build_daily_pool.IBKRPriceAdapter()
        
        # 3. 构建上下文
        logger.info("🔍 构建多粒度上下文...")
        context = build_daily_pool.build_daily_pool_context(pool_config, adapter)
        
        # 4. 获取账户信息（用于风控参数注入）
        account_info = get_account_info()
        context.update(account_info)
        
        # 5. 添加风控参数
        risk_checker_instance = risk_checker.create_risk_checker(date_str)
        context["trades_today"] = risk_checker_instance.today_trade_count
        context["trades_remaining"] = max(0, settings.MAX_DAILY_TRADES - risk_checker_instance.today_trade_count)
        
        # 6. 渲染 Prompt（验证模板）
        logger.info("📝 渲染 Prompt 模板...")
        template = prompts.load_template()
        final_prompt = prompts.render_prompt(template, context)
        logger.info(f"✅ Prompt 长度：{len(final_prompt)} 字符")
        # 🔧 新增：保存盘前 Prompt 日志
        save_prompt_log(final_prompt, cycle=0, date_str=date_str, keep_last_n=3)

        # 7. 保存缓存
        cache_path = build_daily_pool.save_context_cache(context)
        logger.info(f"💾 上下文缓存：{cache_path}")
        
        # 8. 记录决策日志（盘前准备记录）
        perf_logger.log_decision({
            "raw_output": f"Premarket preparation completed",
            "parsed": {"action": "NO_OP"},
            "usage": {"total_tokens": len(final_prompt) // 4},
            "timestamp": datetime.now().isoformat(),
            "simulate": not settings.ENABLE_REAL_TRADING,
            "latency": 0,
            "risk_check_passed": True,
            "executed": False
        })
        
        # 9. 清理
        adapter.disconnect()
        perf_logger.close()
        
        logger.info("=" * 70)
        logger.info("✅ 盘前准备完成")
        logger.info("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 盘前准备失败：{e}", exc_info=True)
        perf_logger.close()
        return False


# ================= 盘中决策模式 =================
def get_est_time() -> datetime:
    """获取当前美东时间（正确处理时区）"""
    # 获取当前 UTC 时间
    utc_now = datetime.now(pytz.UTC)
    
    # 转换为美东时间
    est = pytz.timezone('US/Eastern')
    return utc_now.astimezone(est)

def is_market_hours() -> bool:
    """判断当前是否在常规交易时段（美东时间 9:30 AM - 4:00 PM）"""
    now_est = get_est_time()
    
    # 创建今天的开盘/收盘时间（美东时区）
    market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= now_est < market_close

def is_premarket() -> bool:
    """判断当前是否在盘前时段（美东时间 4:00 AM - 9:30 AM）"""
    now_est = get_est_time()
    
    premarket_start = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
    market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
    
    return premarket_start <= now_est < market_open

def is_afterhours() -> bool:
    """判断当前是否在盘后时段（美东时间 4:00 PM - 8:00 PM）"""
    now_est = get_est_time()
    
    market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
    afterhours_end = now_est.replace(hour=20, minute=0, second=0, microsecond=0)
    
    return market_close <= now_est < afterhours_end

def is_market_hours() -> bool:
    """
    判断当前是否在常规交易时段（美东时间 9:30 AM - 4:00 PM）
    """
    now_est = get_est_time()
    
    # 交易时段：9:30 AM - 4:00 PM
    market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # 🔧 修复：正确处理时间比较
    return market_open <= now_est < market_close

def get_time_to_market_open() -> timedelta:
    """
    获取距离开盘的时间
    Returns:
        timedelta 对象（负数表示已开盘）
    """
    now_est = get_est_time()
    
    # 创建今天 9:30 的美东时间
    market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
    
    # 🔧 修复：如果已开盘，返回负数
    if now_est >= market_open:
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        if now_est < market_close:
            # 已在交易时段内
            return timedelta(seconds=-1)
        else:
            # 已收盘，计算明天开盘
            market_open += timedelta(days=1)
            # 🔧 修复：跳过周末
            while market_open.weekday() >= 5:  # 5=周六，6=周日
                market_open += timedelta(days=1)
    
    return market_open - now_est

def wait_until_market_open() -> bool:
    """
    等待到开盘时间
    Returns:
        True: 成功等待到开盘（或已在交易时段）
        False: 用户中断或等待失败
    """
    global running
    
    # 🔧 关键：先检查是否已在交易时段
    if is_market_hours():
        logger.info("✅ 当前已在交易时段内，无需等待")
        return True
    
    time_to_open = get_time_to_market_open()
    seconds_to_wait = int(time_to_open.total_seconds())
    
    # 🔧 修复：负数表示已开盘，0 表示正好开盘
    if seconds_to_wait <= 0:
        logger.info("✅ 已到达或超过开盘时间")
        return True
    
    # 格式化等待时间
    hours = seconds_to_wait // 3600
    minutes = (seconds_to_wait % 3600) // 60
    
    logger.info("=" * 70)
    logger.info("🌅 盘前等待模式")
    logger.info("=" * 70)
    logger.info(f"📍 当前美东时间：{get_est_time().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"🕘 开盘时间：09:30:00 EST")
    logger.info(f"⏳ 距离开盘：{hours}小时 {minutes}分钟")
    logger.info(f"💡 程序将待机至开盘后自动启动盘中决策")
    logger.info("=" * 70)
    logger.info("按 Ctrl+C 可中断等待")
    logger.info("=" * 70)
    
    # 🔧 修复：倒计时等待逻辑
    last_log_time = datetime.now()
    while seconds_to_wait > 0 and running:
        now = datetime.now()
        
        # 每分钟打印一次状态
        if (now - last_log_time).total_seconds() >= 60:
            time_to_open = get_time_to_market_open()
            seconds_to_wait = max(0, int(time_to_open.total_seconds()))
            hours = seconds_to_wait // 3600
            minutes = (seconds_to_wait % 3600) // 60
            logger.info(f"⏰ 等待中... 剩余：{hours}小时 {minutes}分钟 | "
                       f"美东：{get_est_time().strftime('%H:%M:%S')}")
            last_log_time = now
        
        # 🔧 修复：短时睡眠 + 检查中断
        time.sleep(5)
        seconds_to_wait = int(get_time_to_market_open().total_seconds())
    
    # 🔧 修复：检查退出原因
    if not running:
        logger.info("🚨 用户中断等待")
        return False
    
    if seconds_to_wait <= 0:
        logger.info("✅ 等待完成，已到达开盘时间")
        return True
    
    # 其他情况
    logger.warning("⚠️ 等待异常退出")
    return False

def save_prompt_log(prompt: str, cycle: int, date_str: str):
    """保存 Prompt 到日志文件"""
    from pathlib import Path
    
    prompt_dir = PROJECT_ROOT / "data" / "prompts"
    prompt_dir.mkdir(exist_ok=True)
    
    # 按日期和循环编号保存
    prompt_file = prompt_dir / f"prompt_{date_str.replace('-', '')}_cycle{cycle:04d}.txt"
    
    try:
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(f"# AI-Trader-Intraday Prompt Log\n")
            f.write(f"# Date: {date_str}\n")
            f.write(f"# Cycle: {cycle}\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write(f"# Length: {len(prompt)} chars (~{len(prompt)//4} tokens)\n")
            f.write(f"{'='*70}\n\n")
            f.write(prompt)
        
        logger.info(f"📝 Prompt 已保存：{prompt_file}")
        return str(prompt_file)
    except Exception as e:
        logger.error(f"❌ 保存 Prompt 失败：{e}")
        return None


def save_prompt_log(prompt: str, cycle: int, date_str: str, keep_last_n: int = 10):
    """
    保存 Prompt 到日志文件
    同时清理旧文件，只保留最近 N 个
    """
    from pathlib import Path
    
    prompt_dir = PROJECT_ROOT / "data" / "prompts"
    prompt_dir.mkdir(exist_ok=True)
    
    # 按日期和循环编号保存
    prompt_file = prompt_dir / f"prompt_{date_str.replace('-', '')}_cycle{cycle:04d}.txt"
    
    try:
        with open(prompt_file, 'w', encoding='utf-8') as f:
            f.write(f"# AI-Trader-Intraday Prompt Log\n")
            f.write(f"# Date: {date_str}\n")
            f.write(f"# Cycle: {cycle}\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write(f"# Length: {len(prompt)} chars (~{len(prompt)//4} tokens)\n")
            f.write(f"{'='*70}\n\n")
            f.write(prompt)
        
        logger.info(f"📝 Prompt 已保存：{prompt_file}")
        
        # 🔧 清理旧文件，只保留最近 N 个
        all_prompts = sorted(prompt_dir.glob(f"prompt_{date_str.replace('-', '')}_*.txt"))
        if len(all_prompts) > keep_last_n:
            for old_file in all_prompts[:-keep_last_n]:
                old_file.unlink()
                logger.debug(f"🗑️ 清理旧 Prompt: {old_file}")
        
        return str(prompt_file)
    except Exception as e:
        logger.error(f"❌ 保存 Prompt 失败：{e}")
        return None
    
def run_intraday(pool_path: str, max_cycles: int = None, wait_for_open: bool = True):
    """盘中决策循环"""
    logger.info("=" * 70)
    logger.info("📊 盘中决策模式")
    logger.info("=" * 70)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    perf_logger = PerformanceLogger(date_str)
    
    # 🔧 修复：盘前检测与等待
    if wait_for_open and not is_market_hours():
        if is_premarket():
            logger.warning("⚠️ 当前为盘前时段")
            if not wait_until_market_open():
                logger.info("🚨 等待被中断，退出程序")
                perf_logger.close()
                return False
            logger.info("✅ 已到达开盘时间，开始盘中决策")
        elif is_afterhours():
            logger.warning("⚠️ 当前为盘后时段")
            logger.info("💡 程序将在下一个交易日开盘时自动恢复")
            # 🔧 修复：盘后等待到明天开盘
            if not wait_until_market_open():
                logger.info("🚨 等待被中断，退出程序")
                perf_logger.close()
                return False
        else:
            # 周末或节假日
            logger.warning("⚠️ 当前为非交易时段（可能是周末或节假日）")
            if not wait_until_market_open():
                logger.info("🚨 等待被中断，退出程序")
                perf_logger.close()
                return False
    
    try:
        # 1. 初始化组件
        adapter = build_daily_pool.IBKRPriceAdapter()
        agent = intraday_agent.create_agent()
        parser_instance = parser.create_parser()
        risk_checker_instance = risk_checker.create_risk_checker(date_str)
        executor = ibkr_executor.create_executor()
        
        # 2. 加载资产池
        pool_config = build_daily_pool.parse_stock_pool_template(pool_path)
        symbols = [s["symbol"] for s in pool_config]
        
        # 3. 加载盘前缓存（如果有）
        cache_path = PROJECT_ROOT / "cache" / f"context_{date_str.replace('-', '')}.json"
        if cache_path.exists():
            logger.info(f"📂 加载盘前缓存：{cache_path}")
            import json
            with open(cache_path, 'r') as f:
                context = json.load(f)
        else:
            logger.info("🔄 无盘前缓存，重新构建上下文")
            context = build_daily_pool.build_daily_pool_context(pool_config, adapter)
        
        # 4. 决策循环
        cycle = 0
        while running:
            cycle += 1
            cycle_start = datetime.now()
            
            # 🔧 新增：检查是否已收盘
            if is_market_hours():
                pass  # 交易时段内，继续
            elif wait_for_open:
                logger.info("🌙 已收盘，等待下一个交易日开盘...")
                if not wait_until_market_open():
                    break
            # 如果不等待，继续执行（测试模式）
            
            logger.info(f"\n{'='*70}")
            logger.info(f"🔄 决策循环 {cycle} | 时间：{cycle_start.strftime('%H:%M:%S')} | "
                       f"美东：{get_est_time().strftime('%H:%M:%S')}")
            logger.info(f"{'='*70}")
            
            # 检查最大循环次数
            if max_cycles and cycle > max_cycles:
                logger.info(f"⏹️ 达到最大循环次数：{max_cycles}")
                break
            
            try:
                # ── 步骤 1: 更新 intraday 数据库 ──
                logger.info("📡 更新 intraday 数据库...")
                update_intraday.main(symbols=symbols)
                
                # ── 步骤 2: 刷新上下文（获取最新数据）─ ─
                logger.info("🔄 刷新上下文数据...")
                context = build_daily_pool.build_daily_pool_context(pool_config, adapter)
                
                # 更新账户信息
                account_info = get_account_info()
                context.update(account_info)
                
                # 更新风控参数
                risk_checker_instance = risk_checker.create_risk_checker(date_str)
                context["trades_today"] = risk_checker_instance.today_trade_count
                context["trades_remaining"] = max(0, settings.MAX_DAILY_TRADES - risk_checker_instance.today_trade_count)
                
                # ── 步骤 3: 渲染 Prompt ──
                logger.info("📝 渲染 Prompt...")
                template = prompts.load_template()
                system_prompt = prompts.render_prompt(template, context)
                
                if cycle == 1 or settings.LOG_LEVEL == "DEBUG":
                    save_prompt_log(system_prompt, cycle, date_str, keep_last_n=5)
                    logger.info(f"📊 Prompt 长度：{len(system_prompt)} 字符 (~{len(system_prompt)//4} tokens)")

                # ── 步骤 4: Agent 决策 ──
                logger.info("🤖 Agent 决策中...")
                decision_result = agent.decide(system_prompt)
                decision_result["latency"] = (datetime.now() - cycle_start).total_seconds()
                
                # ── 步骤 5: 解析指令 ──
                logger.info("🔍 解析 LLM 输出...")
                raw_output = decision_result.get("raw_output", "")
                parsed = parser_instance.parse(raw_output)
                decision_result["parsed"] = parsed
                
                logger.info(f"📋 解析结果：{parsed['action']} {parsed.get('symbol', '')} "
                           f"x{parsed.get('qty', '')} @ {parsed.get('price', 'MKT')}")
                
                # ── 步骤 6: 风控校验 ──
                logger.info("🛡️ 风控校验...")
                risk_result = risk_checker_instance.full_check(
                    parsed_instruction=parsed,
                    account_info=account_info,
                    allowed_symbols=symbols
                )
                decision_result["risk_check_passed"] = risk_result["passed"]
                
                if not risk_result["passed"]:
                    logger.warning(f"⚠️ 风控拦截：{risk_result['errors']}")
                    decision_result["executed"] = False
                else:
                    # ── 步骤 7: 执行交易 ──
                    if parsed["action"] == "NO_OP":
                        logger.info("💤 无操作指令")
                        decision_result["executed"] = False
                    else:
                        logger.info(f"📤 执行交易：{parsed['action']} {parsed['symbol']} "
                                   f"x{parsed['qty']} @ {parsed.get('price', 'MKT')}")
                        
                        exec_result = executor.submit_order(
                            symbol=parsed["symbol"],
                            action=parsed["action"],
                            qty=parsed["qty"],
                            price=parsed.get("price"),
                            order_type=parsed.get("order_type", "LMT")
                        )
                        
                        decision_result["executed"] = exec_result["success"]
                        decision_result["order_id"] = exec_result.get("order_id")
                        
                        # 记录交易日志
                        if exec_result["success"]:
                            trade_info = {
                                "symbol": parsed["symbol"],
                                "action": parsed["action"],
                                "quantity": parsed["qty"],
                                "price": parsed.get("price") or exec_result.get("fill_price"),
                                "order_id": exec_result.get("order_id"),
                                "commission": exec_result.get("commission", 0),
                                "pnl": 0,
                                "pnl_percent": 0,
                                "status": exec_result.get("status"),
                                "simulate": exec_result.get("status") == "SIMULATED",
                                "notes": ""
                            }
                            perf_logger.log_trade(trade_info)
                
                # ── 步骤 8: 记录决策日志 ──
                decision_log_id = perf_logger.log_decision(decision_result)
                logger.info(f"📝 决策日志 ID: {decision_log_id}")
                
            except Exception as e:
                logger.error(f"❌ 循环 {cycle} 失败：{e}", exc_info=True)
                perf_logger.log_decision({
                    "raw_output": f"Error: {str(e)}",
                    "parsed": {"action": "NO_OP"},
                    "usage": {},
                    "timestamp": datetime.now().isoformat(),
                    "simulate": not settings.ENABLE_REAL_TRADING,
                    "latency": 0,
                    "risk_check_passed": False,
                    "executed": False,
                    "error": str(e)
                })
            
            # ── 步骤 9: 等待下一个循环 ──
            elapsed = (datetime.now() - cycle_start).total_seconds()
            wait_time = max(0, settings.DECISION_INTERVAL_MINUTES * 60 - elapsed)
            
            if wait_time > 0 and running:
                logger.info(f"💤 等待 {wait_time:.0f} 秒后下一次决策...")
                for _ in range(int(wait_time)):
                    if not running:
                        break
                    time.sleep(1)
        
        # 5. 清理
        adapter.disconnect()
        agent.close()
        executor.disconnect()
        perf_logger.close()
        
        logger.info("=" * 70)
        logger.info(f"✅ 盘中决策结束 | 总循环：{cycle}")
        logger.info("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 盘中决策失败：{e}", exc_info=True)
        perf_logger.close()
        return False


# ================= 盘后复盘模式 =================
def run_review(date_str: str):
    """
    盘后复盘流程：
    1. 加载当日交易记录
    2. 计算绩效指标
    3. 生成复盘报告
    4. 输出总结
    """
    logger.info("=" * 70)
    logger.info("📈 盘后复盘模式")
    logger.info("=" * 70)
    
    if not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    perf_logger = PerformanceLogger(date_str)
    
    try:
        # 1. 获取当日交易记录
        trades = perf_logger.get_daily_trades(date_str)
        decisions = perf_logger.get_daily_decisions(date_str)
        
        logger.info(f"📊 当日交易：{len(trades)} 笔")
        logger.info(f"📊 当日决策：{len(decisions)} 次")
        
        # 2. 计算绩效
        performance = perf_logger.calculate_daily_performance(date_str)
        
        # 3. 保存绩效汇总
        perf_logger.save_daily_performance(performance)
        
        # 4. 生成报告
        report_path = perf_logger.export_daily_report(date_str)
        
        # 5. 输出总结
        print("\n" + "=" * 70)
        print("📊 当日绩效总结")
        print("=" * 70)
        print(f"日期：{date_str}")
        print(f"总交易次数：{performance.get('total_trades', 0)}")
        print(f"盈利交易：{performance.get('winning_trades', 0)}")
        print(f"亏损交易：{performance.get('losing_trades', 0)}")
        print(f"胜率：{performance.get('win_rate', 0)*100:.1f}%")
        print(f"总盈亏：${performance.get('total_pnl', 0):+.2f}")
        print(f"手续费：${performance.get('total_commission', 0):.2f}")
        print(f"净盈亏：${performance.get('net_pnl', 0):+.2f}")
        print(f"最大单笔盈利：${performance.get('max_profit', 0):+.2f}")
        print(f"最大单笔亏损：${performance.get('max_drawdown', 0):+.2f}")
        print(f"Total Tokens 使用：{performance.get('total_tokens_used', 0)}")
        print(f"API 总耗时：{performance.get('total_api_latency_sec', 0):.1f}s")
        print("=" * 70)
        print(f"📄 详细报告：{report_path}")
        print("=" * 70)
        
        perf_logger.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 盘后复盘失败：{e}", exc_info=True)
        perf_logger.close()
        return False


# ================= 辅助函数 =================
def get_account_info() -> Dict[str, Any]:
    """从 IBKR 获取真实账户信息"""
    from ib_insync import IB, Stock
    from config import settings
    
    logger.info("🔍 获取 IBKR 账户信息...")
    
    ib = None
    try:
        ib = IB()
        ib.connect(
            host=settings.IB_HOST,
            port=settings.IB_PORT,
            clientId=settings.IB_CLIENT_ID + 100,
            timeout=10,
            readonly=True
        )
        logger.info(f"✅ IBKR 连接成功：{settings.IB_HOST}:{settings.IB_PORT}")
        
        ib.waitOnUpdate(timeout=5)
        
        account_summary = ib.accountSummary()
        
        total_value = 0.0
        available_cash = 0.0
        total_cash = 0.0
        account_code = None
        
        logger.info(f"📋 获取到 {len(account_summary)} 条账户摘要记录")
        
        for summary in account_summary:
            if account_code is None:
                account_code = summary.account
                logger.info(f"📋 账户代码：{account_code}")
            
            if summary.account != account_code:
                continue
            
            # 🔧 修复：放宽 tag 过滤条件
            if summary.tag == 'NetLiquidation':  # 不限制 currency
                total_value = float(summary.value)
                logger.info(f"💰 账户总值：${total_value:,.2f} ({summary.currency})")
            elif summary.tag == 'AvailableFunds':
                available_cash = float(summary.value)
                logger.info(f"💵 可用现金：${available_cash:,.2f} ({summary.currency})")
            elif summary.tag == 'TotalCashValue':
                total_cash = float(summary.value)
                logger.info(f"💰 总现金：${total_cash:,.2f} ({summary.currency})")
        
        # 🔧 修复：如果 USD 有数据则使用
        if total_value == 0:
            for summary in account_summary:
                if summary.account != account_code:
                    continue
                if summary.tag == 'NetLiquidation' and summary.currency == 'USD':
                    total_value = float(summary.value)
                elif summary.tag == 'AvailableFunds' and summary.currency == 'USD':
                    available_cash = float(summary.value)
        
        # 获取持仓
        positions = ib.positions()
        logger.info(f"📊 获取到 {len(positions)} 个持仓")
        
        positions_dict = {}
        current_prices = {}
        
        for pos in positions:
            if pos.account != account_code:
                continue
            
            if pos.contract.symbol and pos.contract.secType == 'STK':
                symbol = pos.contract.symbol
                shares = pos.position
                avg_cost = pos.avgCost
                
                if shares == 0:
                    continue
                
                try:
                    contract = Stock(symbol, "SMART", "USD")
                    ib.reqMktData(contract, '', False, False)
                    ib.sleep(0.2)
                    
                    ticker = ib.ticker(contract)
                    current_price = ticker.last if ticker.last else ticker.close
                    
                    if current_price and current_price > 0:
                        current_prices[symbol] = current_price
                    else:
                        # 🔧 修复：avgCost 是总成本，不是每股成本
                        current_prices[symbol] = avg_cost
                        
                except Exception as e:
                    logger.warning(f"⚠️ 获取 {symbol} 价格失败：{e}")
                    current_prices[symbol] = avg_cost
                
                # 🔧 修复：正确计算每股成本
                cost_per_share = avg_cost
                current_price = current_prices.get(symbol, 0)
                market_value = current_price * abs(shares)
                total_cost = cost_per_share * abs(shares)
                pnl = (current_price - cost_per_share) * abs(shares)
                pnl_pct = (current_price / cost_per_share - 1) * 100 if cost_per_share > 0 else 0
                
                positions_dict[symbol] = {
                    "shares": int(abs(shares)),
                    "avg_cost": cost_per_share,  # 🔧 每股成本
                    "current_price": current_price,
                    "market_value": market_value,
                    "total_cost": total_cost,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct
                }
                logger.info(f"  📈 {symbol}: {int(abs(shares))} shares @ ${current_price:.2f} (cost: ${cost_per_share:.2f})")
        
        positions_dict["CASH"] = {
            "available": available_cash,
            "total": total_cash
        }
        
        # 计算收益率
        if total_value > 0 and total_cash > 0:
            total_return = (total_value - total_cash) / total_cash * 100
        else:
            total_return = 0
        
        logger.info(f"✅ 账户信息获取完成 | 总值：${total_value:,.2f} | 现金：${available_cash:,.2f} | 持仓：{len(positions_dict) - 1} 只")
        
        return {
            "total_value": total_value,
            "available_cash": available_cash,
            "return": total_return / 100,
            "positions": positions_dict,
            "current_prices": current_prices
        }
        
    except Exception as e:
        logger.error(f"❌ 获取账户信息失败：{e}", exc_info=True)
        logger.warning("⚠️ 使用模拟账户数据")
        return {
            "total_value": 100000.0,
            "available_cash": 50000.0,
            "return": 0.05,
            "positions": {
                "AAPL": {"shares": 100, "avg_cost": 150.0, "current_price": 155.0},
                "MSFT": {"shares": 50, "avg_cost": 300.0, "current_price": 305.0},
                "CASH": {"available": 50000.0}
            },
            "current_prices": {
                "AAPL": 155.0,
                "MSFT": 305.0,
                "TSLA": 200.0,
                "NVDA": 500.0
            }
        }
    finally:
        if ib and ib.isConnected():
            ib.disconnect()
            logger.info("🔌 IBKR 账户查询连接已关闭")


def validate_pool_path(pool_path: str) -> str:
    """验证资产池路径"""
    path = Path(pool_path)
    if not path.exists():
        # 尝试默认路径
        default_path = PROJECT_ROOT / "config" / "stock_pool_template.txt"
        if default_path.exists():
            logger.info(f"⚠️ 指定路径不存在，使用默认：{default_path}")
            return str(default_path)
        raise FileNotFoundError(f"资产池文件不存在：{pool_path}")
    return str(path)


# ================= 主入口 =================
def main():
    parser = argparse.ArgumentParser(
        description="AI-Trader-Intraday 统一入口",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 盘前准备
  python main.py --mode=premarket --pool=config/stock_pool_template.txt
  
  # 盘中决策（单次）
  python main.py --mode=intraday --pool=config/stock_pool_template.txt --cycles=1
  
  # 盘中决策（持续循环）
  python main.py --mode=intraday --pool=config/stock_pool_template.txt
  
  # 盘后复盘
  python main.py --mode=review --date=20260328
        """
    )
    
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["premarket", "intraday", "review"],
        help="运行模式"
    )
    
    parser.add_argument(
        "--pool",
        type=str,
        default="config/stock_pool_template.txt",
        help="资产池模板路径"
    )
    
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="日期（YYYYMMDD 格式，用于 review 模式）"
    )
    
    parser.add_argument(
        "--cycles",
        type=int,
        default=None,
        help="最大决策循环次数（仅 intraday 模式）"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="日志级别"
    )
    
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="盘前时不等待开盘，立即开始（用于测试）"
    )
    
    args = parser.parse_args()
    
    # 设置日志级别
    settings.LOG_LEVEL = args.log_level
    
    # 日期处理
    date_str = args.date
    if date_str and len(date_str) == 8:
        # YYYYMMDD → YYYY-MM-DD
        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    elif not date_str:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    # 设置日志
    setup_logging(args.mode, date_str)
    
    # 注册信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    global current_mode
    current_mode = args.mode
    
    # 执行对应模式
    success = False
    
    if args.mode == "premarket":
        pool_path = validate_pool_path(args.pool)
        success = run_premarket(pool_path)
        
    elif args.mode == "intraday":
        pool_path = validate_pool_path(args.pool)
        success = run_intraday(
            pool_path, 
            max_cycles=args.cycles,
            wait_for_open=not args.no_wait  # 默认等待开盘
        )
        
    elif args.mode == "review":
        success = run_review(date_str)
    
    # 退出
    if success:
        logger.info("✅ 任务成功完成")
        sys.exit(0)
    else:
        logger.error("❌ 任务失败")
        sys.exit(1)


if __name__ == "__main__":
    main()