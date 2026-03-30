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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import settings
from src.monitor.performance_logger import PerformanceLogger

# 延迟导入（避免循环依赖）
def import_modules():
    global build_daily_pool, prompts, intraday_agent, parser, risk_checker, ibkr_executor
    
    from src.data_prep import build_daily_pool
    from src.agent import prompts, intraday_agent, parser
    from src.execution import risk_checker, ibkr_executor

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


# ================= 盘前准备模式 =================
def run_premarket(pool_path: str):
    """
    盘前准备流程：
    1. 解析资产池模板
    2. 查询多粒度历史数据
    3. 构建 Prompt 上下文
    4. 保存缓存供盘中使用
    """
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
def run_intraday(pool_path: str, max_cycles: int = None):
    """
    盘中决策循环：
    1. 更新 intraday 数据库（5 分钟/1 小时/2 小时）
    2. 加载/刷新上下文
    3. Agent 决策
    4. 解析指令
    5. 风控校验
    6. 执行交易
    7. 记录日志
    
    每 5 分钟循环一次
    """
    logger.info("=" * 70)
    logger.info("📊 盘中决策模式")
    logger.info("=" * 70)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    perf_logger = PerformanceLogger(date_str)
    
    # 导入模块
    import_modules()
    from src.data_prep import update_intraday
    
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
            logger.info(f"\n{'='*70}")
            logger.info(f"🔄 决策循环 {cycle} | 时间：{cycle_start.strftime('%H:%M:%S')}")
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
                                "pnl": 0,  # 买入时 PnL 为 0
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
                # 记录错误日志
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
                # 可中断的等待
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
    """
    获取账户信息
    实际使用时应从 IBKR API 或本地持仓记录获取
    这里返回模拟数据供测试
    """
    # TODO: 实现真实的 IBKR 账户查询
    return {
        "total_value": 100000.0,
        "available_cash": 50000.0,
        "return": 0.05,  # 5%
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
        success = run_intraday(pool_path, max_cycles=args.cycles)
        
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