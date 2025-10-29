import os
import json
import time
import datetime as dt
from typing import Optional, Tuple

from tenacity import retry, wait_exponential, stop_after_attempt
import gspread
from google.oauth2.service_account import Credentials

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest
from alpaca.common.types import BaseURL
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.data.models import Trade


# ----------------------------
# ENV VARS
# ----------------------------
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET", "")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
SHEET_NAME = os.getenv("SHEET_NAME", "Active-Investing")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Alpaca Integration")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "")

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "3.0"))
PROFIT_ARM_PCT = float(os.getenv("PROFIT_ARM_PCT", "5.0"))
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "2.0"))

# Sheet columns
COL_TICKER, COL_COST, COL_HWM, COL_ACTION, COL_TRIGGER, COL_TIME = 3, 4, 5, 6, 7, 8


# ----------------------------
# Helpers
# ----------------------------
def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def parse_float_or_none(s: str) -> Optional[float]:
    try:
        s = (s or "").strip()
        return float(s) if s else None
    except Exception:
        return None


def pct_change(from_price: float, to_price: float) -> float:
    return (to_price - from_price) / from_price * 100 if from_price else 0.0


# ----------------------------
# Google Sheets setup
# ----------------------------
def make_gspread_client() -> gspread.Client:
    info = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_worksheet(gc: gspread.Client):
    return gc.open(SHEET_NAME).worksheet(WORKSHEET_NAME)


# ----------------------------
# Alpaca setup
# ----------------------------
def make_alpaca_clients() -> Tuple[TradingClient, StockHistoricalDataClient]:
    trading = TradingClient(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_API_SECRET,
        paper=("paper" in ALPACA_BASE_URL),
        base_url=BaseURL(ALPACA_BASE_URL),
    )
    data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)
    return trading, data_client


@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(5))
def get_latest_trade_price(data_client: StockHistoricalDataClient, symbol: str) -> float:
    req = StockLatestTradeRequest(symbol_or_symbols=symbol)
    res = data_client.get_stock_latest_trade(req)
    trade = res[symbol] if isinstance(res, dict) else res
    return float(trade.price)


def get_position_qty(trading: TradingClient, symbol: str) -> float:
    try:
        pos = trading.get_open_position(symbol)
        return float(pos.qty)
    except Exception:
        return 0.0


def place_market_sell(trading: TradingClient, symbol: str, qty: float):
    order_req = MarketOrderRequest(
        symbol=symbol, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY
    )
    return trading.submit_order(order_data=order_req)


def write_row_cells(ws, row_idx: int, action: str, trigger: str):
    ws.update_cell(row_idx, COL_ACTION, action)
    ws.update_cell(row_idx, COL_TRIGGER, trigger)
    ws.update_cell(row_idx, COL_TIME, now_utc_iso())


# ----------------------------
# New: remove sold ticker row
# ----------------------------
def remove_row(ws, row_idx: int):
    try:
        ws.delete_rows(row_idx)
        print(f"[INFO] Removed sold row {row_idx}")
    except Exception as e:
        print(f"[WARN] Failed to remove row {row_idx}: {e}")


# ----------------------------
# Core logic
# ----------------------------
def process_row(ws, trading, data_client, row_idx: int):
    ticker = (ws.cell(row_idx, COL_TICKER).value or "").strip().upper()
    if not ticker:
        return

    cost = parse_float_or_none(ws.cell(row_idx, COL_COST).value)
    if not cost:
        write_row_cells(ws, row_idx, "SKIP", "Invalid cost")
        return

    qty = get_position_qty(trading, ticker)
    if qty <= 0:
        write_row_cells(ws, row_idx, "NO POSITION", "Qty=0")
        return

    try:
        price = get_latest_trade_price(data_client, ticker)
    except Exception as e:
        write_row_cells(ws, row_idx, "PRICE ERROR", str(e))
        return

    hwm = parse_float_or_none(ws.cell(row_idx, COL_HWM).value)
    change_pct = pct_change(cost, price)
    trigger_note = f"Δ={change_pct:.2f}% @ {price:.4f}"

    # Arm or raise high-water mark
    if change_pct >= PROFIT_ARM_PCT:
        if hwm is None or price > hwm:
            ws.update_cell(row_idx, COL_HWM, f"{price:.6f}")
            hwm = price
        action = "ARMED"
    else:
        if hwm is not None:
            ws.update_cell(row_idx, COL_HWM, "")
            hwm = None
        action = "HOLD"

    # Trailing stop logic
    if hwm is not None and price <= hwm * (1 - TRAIL_PCT / 100):
        try:
            place_market_sell(trading, ticker, qty)
            write_row_cells(ws, row_idx, "SELL: TRAIL", f"HWM={hwm:.4f} hit @ {price:.4f}")
            remove_row(ws, row_idx)  # ← remove after sell
            return True
        except Exception as e:
            write_row_cells(ws, row_idx, "SELL ERROR", f"TRAIL: {e}")
            return False

    # Hard stop-loss logic
    if change_pct <= -STOP_LOSS_PCT:
        try:
            place_market_sell(trading, ticker, qty)
            write_row_cells(ws, row_idx, "SELL: STOP", trigger_note)
            remove_row(ws, row_idx)  # ← remove after sell
            return True
        except Exception as e:
            write_row_cells(ws, row_idx, "SELL ERROR", f"STOP: {e}")
            return False

    write_row_cells(ws, row_idx, action, trigger_note)
    return False


def run_once(ws, trading, data_client):
    col_c = ws.col_values(COL_TICKER)
    last_row = len(col_c)
    if last_row < 2:
        return

    row_idx = 2
    while row_idx <= last_row:
        removed = process_row(ws, trading, data_client, row_idx)
        if removed:
            # Row deleted → do not increment; next row shifts up
            last_row -= 1
        else:
            row_idx += 1


def main():
    print("[BOOT] Starting Alpaca-Integration bot")
    gc = make_gspread_client()
    ws = get_worksheet(gc)
    trading, data_client = make_alpaca_clients()

    print(f"[READY] Monitoring '{SHEET_NAME}' → '{WORKSHEET_NAME}' every {POLL_SECONDS}s")
    while True:
        try:
            run_once(ws, trading, data_client)
        except Exception as e:
            print(f"[ERROR] Loop error: {e}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
