"""
Perpetual Arbitrage Bot - Cross-exchange arbitrage trading
"""

import os
import time
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Literal, Tuple, Optional

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot


@dataclass
class PerpArbConfig:
    """Configuration class for perpetual arbitrage trading."""
    ticker: str
    contract_id_leg1: str
    contract_id_leg2: str
    exchange_leg1: str
    exchange_leg2: str
    max_quantity: Decimal          # 单次套利下单的最大数量
    max_total_size: Decimal        # 持仓总数量上限
    order_interval: int            # 下单最小时间间隔 ms
    loop_interval: int             # 轮询时间间隔 ms
    order_timeout: int             # 订单超时时间 ms
    min_price_diff_open: Decimal   # 套利开仓的最小价差
    max_price_diff_close: Decimal  # 套利关仓的最大价差
    min_order_size: Decimal        # 下单的最小数量
    tick_size_leg1: Decimal = Decimal('0')
    tick_size_leg2: Decimal = Decimal('0')
    close_order_side: str = "" 


@dataclass
class OrderState:
    """Order execution state tracking."""
    leg1_success: bool = False
    leg2_success: bool = False
    leg1_filled_size: Decimal = Decimal('0')
    leg2_filled_size: Decimal = Decimal('0')
    leg1_order_id: Optional[str] = None
    leg2_order_id: Optional[str] = None


class PerpArbBot:
    """Perpetual Arbitrage Trading Bot."""

    def __init__(self, config: PerpArbConfig):
        self.config = config
        self.logger = TradingLogger(
            exchange=f"PERP_ARB_{config.exchange_leg1}_{config.exchange_leg2}",
            ticker=config.ticker,
            log_to_console=True
        )

        # Create exchange clients for both legs
        self.leg1_client = None
        self.leg2_client = None
        
        # Trading state
        self.shutdown_requested = False
        self.last_order_time = 0
        self.loop = None
        
        # Order tracking
        self.pending_orders = {}  # {order_id: {'leg': 1|2, 'direction': 'buy'|'sell', 'size': Decimal}}

    async def _initialize_clients(self):
        """Initialize both exchange clients."""
        try:
            # Create leg1 client
            leg1_config_dict = {
                'ticker': self.config.ticker,
                'contract_id': self.config.contract_id_leg1,
                'quantity': self.config.max_quantity,
                'tick_size': self.config.tick_size_leg1,
                'direction': 'buy',  # Will be overridden per trade
            }
            
            self.leg1_client = ExchangeFactory.create_exchange(
                self.config.exchange_leg1,
                type('Config', (), leg1_config_dict)(),
                self.logger,
            )
            
            # Create leg2 client
            leg2_config_dict = {
                'ticker': self.config.ticker,
                'contract_id': self.config.contract_id_leg2,
                'quantity': self.config.max_quantity,
                'tick_size': self.config.tick_size_leg2,
                'direction': 'sell',  # Will be overridden per trade
            }
            
            self.leg2_client = ExchangeFactory.create_exchange(
                self.config.exchange_leg2,
                type('Config', (), leg2_config_dict)(),
                self.logger,
            )
            
            self.logger.log("Exchange clients initialized", "INFO")
            
        except Exception as e:
            self.logger.log(f"Failed to initialize clients: {e}", "ERROR")
            raise

    async def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for both legs."""
        
        def create_order_handler(leg_num: int):
            def handler(message):
                try:
                    order_id = message.get('order_id')
                    status = message.get('status')
                    
                    if order_id in self.pending_orders:
                        order_info = self.pending_orders[order_id]
                        
                        if status == 'FILLED':
                            filled_size = Decimal(message.get('filled_size', 0))
                            order_info['filled_size'] = filled_size
                            order_info['status'] = 'FILLED'
                            
                            self.logger.log(
                                f"[LEG{leg_num}] [{order_id}] FILLED "
                                f"{filled_size} @ {message.get('price')}",
                                "INFO"
                            )
                            
                        elif status in ['CANCELED', 'FAILED']:
                            order_info['status'] = status
                            self.logger.log(
                                f"[LEG{leg_num}] [{order_id}] {status}",
                                "WARNING"
                            )
                            
                except Exception as e:
                    self.logger.log(f"Error in order handler (leg{leg_num}): {e}", "ERROR")
            
            return handler
        
        # Setup handlers for both legs
        self.leg1_client.setup_order_update_handler(create_order_handler(1))
        self.leg2_client.setup_order_update_handler(create_order_handler(2))

    async def _get_positions(self) -> Tuple[Decimal, Decimal]:
        """Get positions for both legs."""
        try:
            leg1_pos = await self.leg1_client.get_account_positions()
            leg2_pos = await self.leg2_client.get_account_positions()
            return leg1_pos, leg2_pos
        except Exception as e:
            self.logger.log(f"Error getting positions: {e}", "ERROR")
            return Decimal('0'), Decimal('0')

    async def _fetch_depths(self) -> Dict[str, Dict[str, Tuple[Decimal, Decimal]]]:
        """Fetch orderbook depths for both legs and organize by direction."""
        try:
            # Get best bid/ask for both legs
            leg1_bid, leg1_ask, leg1_bid_qty, leg1_ask_qty  = await self.leg1_client.fetch_bbo_prices_quanities(self.config.contract_id_leg1)
            leg2_bid, leg2_ask, leg2_bid_qty, leg2_ask_qty = await self.leg2_client.fetch_bbo_prices_quanities(self.config.contract_id_leg2)
            
            # Validate prices
            if (leg1_bid <= 0 or leg1_ask <= 0 or leg2_bid <= 0 or leg2_ask <= 0 or
                leg1_bid >= leg1_ask or leg2_bid >= leg2_ask):
                raise ValueError("Invalid bid/ask prices")
            
            best_orders = {
                "12": {  # leg1 buy, leg2 sell
                    "best_ask": [leg1_ask, leg1_ask_qty],  # leg1 taker buy price
                    "best_bid": [leg2_bid, leg1_bid_qty],  # leg2 taker sell price
                },
                "21": {  # leg1 sell, leg2 buy
                    "best_bid": [leg1_bid, leg2_ask_qty],  # leg1 taker sell price
                    "best_ask": [leg2_ask, leg2_bid_qty],  # leg2 taker buy price
                }
            }
            
            return best_orders
            
        except Exception as e:
            self.logger.log(f"Error fetching depths: {e}", "ERROR")
            raise

    def _calculate_price_diff(self, best_orders: Dict) -> Dict[str, Decimal]:
        """Calculate price difference for each direction."""
        if best_orders["12"]["best_bid"][0] * best_orders["12"]["best_ask"][0] * best_orders["21"]["best_bid"][0] * best_orders["21"]["best_ask"][0] == Decimal("0"):
            self.logger.log(
                f'_calculate_price_diff Error: best price is 0.\n'
                f'12_best_bid: {best_orders["12"]["best_bid"][0]} 12_best_ask: {best_orders["12"]["best_ask"][0]}\n'
                f'21_best_bid: {best_orders["21"]["best_bid"][0]} 21_best_ask: {best_orders["21"]["best_ask"][0]}',
                "ERROR"
            )
            raise ValueError("best price is 0")
        price_diff = {}
        
        # Direction "12": leg1 buy (ask), leg2 sell (bid)
        # price_diff = sell_price - buy_price
        price_diff["12"] = best_orders["12"]["best_bid"][0] - best_orders["12"]["best_ask"][0]
        
        # Direction "21": leg1 sell (bid), leg2 buy (ask)
        # price_diff = sell_price - buy_price
        price_diff["21"] = best_orders["21"]["best_bid"][0] - best_orders["21"]["best_ask"][0]
        
        return price_diff

    async def _check_arb_conditions(
        self,
        price_diff: Dict[str, Decimal],
        leg1_pos: Decimal,
        leg2_pos: Decimal
    ) -> Tuple[Optional[str], Optional[Literal["OPEN", "CLOSE"]]]:
        """Check if arbitrage conditions are met."""
        
        max_pos = max(abs(leg1_pos), abs(leg2_pos))
        min_pos = min(abs(leg1_pos), abs(leg2_pos))
        
        # Check each direction
        for direction in ["12", "21"]:
            diff = price_diff[direction]
            
            # Check open conditions
            if max_pos < self.config.max_total_size - self.config.min_order_size:
                if diff >= self.config.min_price_diff_open:
                    self.logger.log(
                        f"OPEN opportunity found: direction={direction}, "
                        f"price_diff={diff}, positions=({leg1_pos}, {leg2_pos})",
                        "INFO"
                    )
                    return direction, "OPEN"
            
            # Check close conditions
            if min_pos >= self.config.min_order_size:
                if diff <= self.config.max_price_diff_close:
                    self.logger.log(
                        f"CLOSE opportunity found: direction={direction}, "
                        f"price_diff={diff}, positions=({leg1_pos}, {leg2_pos})",
                        "INFO"
                    )
                    return direction, "CLOSE"
        
        return None, None

    async def _execute_orders(
        self,
        direction: str,
        action: Literal["OPEN", "CLOSE"],
        best_orders: Dict
    ) -> OrderState:
        """Execute orders on both legs simultaneously."""
        
        # Calculate order quantity
        if direction == "12":
            leg1_available = best_orders["12"]["best_ask"][1]
            leg2_available = best_orders["12"]["best_bid"][1]
        else:  # "21"
            leg1_available = best_orders["21"]["best_bid"][1]
            leg2_available = best_orders["21"]["best_ask"][1]
        
        order_quantity = min(
            leg1_available,
            leg2_available,
            self.config.max_quantity
        )
        
        # Skip if quantity too small
        if order_quantity < self.config.min_order_size:
            self.logger.log(
                f"Order quantity {order_quantity} < min_order_size {self.config.min_order_size}, skipping",
                "INFO"
            )
            return OrderState()
        
        # Determine order directions for each leg
        if action == "OPEN":
            if direction == "12":
                leg1_direction = "buy"   # leg1 long
                leg2_direction = "sell"  # leg2 short
            else:  # "21"
                leg1_direction = "sell"  # leg1 short
                leg2_direction = "buy"   # leg2 long
        else:  # CLOSE
            if direction == "12":
                leg1_direction = "sell"  # close leg1 long
                leg2_direction = "buy"   # close leg2 short
            else:  # "21"
                leg1_direction = "buy"   # close leg1 short
                leg2_direction = "sell"  # close leg2 long
        
        self.logger.log(
            f"Executing {action} orders: direction={direction}, "
            f"quantity={order_quantity}, leg1={leg1_direction}, leg2={leg2_direction}",
            "INFO"
        )
        
        # Place orders simultaneously with retry logic
        order_state = await self._place_orders_with_retry(
            order_quantity,
            leg1_direction,
            leg2_direction,
            max_retries=3,
            reduce_only= action == "CLOSE"
        )
        
        return order_state

    async def _place_orders_with_retry(
        self,
        quantity: Decimal,
        leg1_direction: str,
        leg2_direction: str,
        max_retries: int = 3,
        reduce_only: bool = False,
    ) -> OrderState:
        """Place orders on both legs with retry logic."""
        
        order_state = OrderState()
        
        for attempt in range(max_retries):
            try:
                # Place orders simultaneously
                results = await asyncio.gather(
                    self.leg1_client.place_market_order(
                        self.config.contract_id_leg1,
                        quantity,
                        leg1_direction,
                        reduce_only,
                    ),
                    self.leg2_client.place_market_order(
                        self.config.contract_id_leg2,
                        quantity,
                        leg2_direction,
                        reduce_only,
                    ),
                    return_exceptions=True
                )
                
                leg1_result, leg2_result = results
                
                # Process leg1 result
                if isinstance(leg1_result, Exception):
                    self.logger.log(f"[LEG1] Order failed: {leg1_result}", "ERROR")
                    order_state.leg1_success = False
                elif leg1_result.success:
                    order_state.leg1_success = True
                    order_state.leg1_filled_size = leg1_result.size
                    order_state.leg1_order_id = leg1_result.order_id
                    self.logger.log(
                        f"[LEG1] Order filled: {leg1_result.size} @ {leg1_result.price}",
                        "INFO"
                    )
                else:
                    self.logger.log(f"[LEG1] Order failed: {leg1_result.error_message}", "ERROR")
                    order_state.leg1_success = False
                
                # Process leg2 result
                if isinstance(leg2_result, Exception):
                    self.logger.log(f"[LEG2] Order failed: {leg2_result}", "ERROR")
                    order_state.leg2_success = False
                elif leg2_result.success:
                    order_state.leg2_success = True
                    order_state.leg2_filled_size = leg2_result.size
                    order_state.leg2_order_id = leg2_result.order_id
                    self.logger.log(
                        f"[LEG2] Order filled: {leg2_result.size} @ {leg2_result.price}",
                        "INFO"
                    )
                else:
                    self.logger.log(f"[LEG2] Order failed: {leg2_result.error_message}", "ERROR")
                    order_state.leg2_success = False
                
                # Both orders failed
                if not order_state.leg1_success and not order_state.leg2_success:
                    self.logger.log("Both orders failed, stopping retries", "ERROR")
                    break
                
                # Both orders succeeded
                if order_state.leg1_success and order_state.leg2_success:
                    self.logger.log("Both orders succeeded", "INFO")
                    break
                
                # One order failed - retry with market order to close risk exposure
                if attempt < max_retries - 1:
                    self.logger.log(
                        f"One order failed (attempt {attempt + 1}/{max_retries}), retrying...",
                        "WARNING"
                    )
                    
                    # Retry only the failed leg with market order
                    if not order_state.leg1_success:
                        retry_result = await self.leg1_client.place_market_order(
                            self.config.contract_id_leg1,
                            quantity,
                            leg1_direction,
                            reduce_only,
                        )
                        if retry_result.success:
                            order_state.leg1_success = True
                            order_state.leg1_filled_size = retry_result.size
                            order_state.leg1_order_id = retry_result.order_id
                    
                    if not order_state.leg2_success:
                        retry_result = await self.leg2_client.place_market_order(
                            self.config.contract_id_leg2,
                            quantity,
                            leg2_direction,
                            reduce_only,
                        )
                        if retry_result.success:
                            order_state.leg2_success = True
                            order_state.leg2_filled_size = retry_result.size
                            order_state.leg2_order_id = retry_result.order_id
                    
                    # Check if retry succeeded
                    if order_state.leg1_success and order_state.leg2_success:
                        self.logger.log("Retry succeeded", "INFO")
                        break
                
            except Exception as e:
                self.logger.log(f"Error placing orders (attempt {attempt + 1}): {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
        
        return order_state

    async def _check_mismatch(self, leg1_pos: Decimal, leg2_pos: Decimal) -> bool:
        """Check position mismatch and handle accordingly."""
        
        mismatch_size = abs(abs(leg1_pos) - abs(leg2_pos))
        
        if mismatch_size < self.config.min_order_size:
            # Ignore small mismatch
            return False
        
        elif mismatch_size >= self.config.max_quantity:
            # Critical mismatch - exit script
            error_msg = (
                f"\n\n[CRITICAL ERROR] Position Mismatch Detected\n"
                f"Exchange: {self.config.exchange_leg1} / {self.config.exchange_leg2}\n"
                f"Ticker: {self.config.ticker}\n"
                f"Leg1 Position: {leg1_pos}\n"
                f"Leg2 Position: {leg2_pos}\n"
                f"Mismatch Size: {mismatch_size}\n"
                f"Max Quantity: {self.config.max_quantity}\n"
                f"⚠️ Mismatch exceeds max_quantity threshold!\n"
                f"⚠️ Script will exit. Please manually rebalance positions.\n"
            )
            
            self.logger.log(error_msg, "ERROR")
            await self.send_notification(error_msg)
            
            return True
        
        return False

    async def send_notification(self, message: str):
        """Send notification via Lark and Telegram."""
        try:
            lark_token = os.getenv("LARK_TOKEN")
            if lark_token:
                async with LarkBot(lark_token) as lark_bot:
                    await lark_bot.send_text(message)
            
            telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
            telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
            if telegram_token and telegram_chat_id:
                with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                    tg_bot.send_text(message)
        except Exception as e:
            self.logger.log(f"Error sending notification: {e}", "ERROR")

    async def run(self):
        """Main trading loop."""
        try:
            # Initialize clients
            await self._initialize_clients()
            
            # Get contract attributes
            _, self.config.tick_size_leg1 = \
                await self.leg1_client.get_contract_attributes()
            _, self.config.tick_size_leg2 = \
                await self.leg2_client.get_contract_attributes()
            
            # Log configuration
            self.logger.log("=== Perpetual Arbitrage Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Leg1: {self.config.exchange_leg1} - {self.config.contract_id_leg1}", "INFO")
            self.logger.log(f"Leg2: {self.config.exchange_leg2} - {self.config.contract_id_leg2}", "INFO")
            self.logger.log(f"Max Quantity: {self.config.max_quantity}", "INFO")
            self.logger.log(f"Max Total Size: {self.config.max_total_size}", "INFO")
            self.logger.log(f"Min Price Diff (Open): {self.config.min_price_diff_open}", "INFO")
            self.logger.log(f"Max Price Diff (Close): {self.config.max_price_diff_close}", "INFO")
            self.logger.log(f"Min Order Size: {self.config.min_order_size}", "INFO")
            self.logger.log("========================================", "INFO")
            
            # Connect to exchanges
            await self.leg1_client.connect()
            await self.leg2_client.connect()
            
            # Setup WebSocket handlers
            await self._setup_websocket_handlers()
            
            # Wait for connections to establish
            await asyncio.sleep(5)
            
            # Capture event loop
            self.loop = asyncio.get_running_loop()
            
            # Initial position check
            leg1_pos, leg2_pos = await self._get_positions()
            mismatch_size = abs(abs(leg1_pos) - abs(leg2_pos))
            self.logger.log(f"mismatch_size {mismatch_size}")
            
            # if mismatch_size >= self.config.min_order_size:
            #     error_msg = (
            #         f"\n\n【WARNING】Initial Position Mismatch Detected\n"
            #         f"Leg1 Position: {leg1_pos}\n"
            #         f"Leg2 Position: {leg2_pos}\n"
            #         f"Mismatch Size: {mismatch_size}\n"
            #         f"⚠️ Please manually rebalance before starting bot.\n"
            #     )
            #     self.logger.log(error_msg, "WARNING")
            #     await self.send_notification(error_msg)
                
            #     if mismatch_size >= self.config.max_quantity:
            #         raise ValueError("Initial mismatch too large, exiting")
            
            # Main trading loop
            while not self.shutdown_requested:
                try:
                    # Check order interval
                    current_time = time.time() * 1000  # Convert to ms
                    if current_time - self.last_order_time < self.config.order_interval:
                        await asyncio.sleep(self.config.loop_interval / 1000)
                        continue
                    
                    # 1. Get positions
                    leg1_pos, leg2_pos = await self._get_positions()
                    
                    # 2. Fetch depths
                    best_orders = await self._fetch_depths()
                    
                    # 3. Calculate price differences
                    price_diff = self._calculate_price_diff(best_orders)
                    
                    # 4. Check arbitrage conditions
                    direction, action = await self._check_arb_conditions(
                        price_diff,
                        leg1_pos,
                        leg2_pos
                    )
                    
                    # Log current state
                    self.logger.log(
                        f"Positions: L1={leg1_pos}, L2={leg2_pos} | "
                        f"Price Diff: 12={price_diff['12']}, 21={price_diff['21']} | "
                        f"direction: {direction}, action: {action}",
                        "INFO"
                    )
                    
                    # # 5. Execute orders if conditions met
                    # if direction and action:
                    #     order_state = await self._execute_orders(
                    #         direction,
                    #         action,
                    #         best_orders
                    #     )
                        
                    #     self.last_order_time = time.time() * 1000
                        
                    #     # 6. Check mismatch after order execution
                    #     leg1_pos, leg2_pos = await self._get_positions()
                    #     critical_mismatch = await self._check_mismatch(leg1_pos, leg2_pos)
                        
                    #     if critical_mismatch:
                    #         self.shutdown_requested = True
                    #         break
                    
                    # Wait before next loop
                    await asyncio.sleep(self.config.loop_interval / 1000)
                    
                except Exception as e:
                    self.logger.log(f"Error in trading loop: {e}", "ERROR")
                    self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
                    await asyncio.sleep(5)
            
        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user", "INFO")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            raise
        finally:
            # Cleanup
            try:
                if self.leg1_client:
                    await self.leg1_client.disconnect()
                if self.leg2_client:
                    await self.leg2_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error during cleanup: {e}", "ERROR")


async def main():
    """Entry point."""
    # Example configuration
    config = PerpArbConfig(
        ticker="ETH",
        contract_id_leg1="ETHUSDC",
        contract_id_leg2="0",
        exchange_leg1="binance",
        exchange_leg2="lighter",
        max_quantity=Decimal("0.1"),
        max_total_size=Decimal("1.0"),
        order_interval=1*1000,      # 1 second
        loop_interval=1*1000,        # 1 second
        order_timeout=10*1000,       # 10 seconds
        min_price_diff_open=Decimal("5"),
        max_price_diff_close=Decimal("1.0"),
        min_order_size=Decimal("0.025")
    )
    
    bot = PerpArbBot(config)
    await bot.run()


if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv()

    asyncio.run(main())