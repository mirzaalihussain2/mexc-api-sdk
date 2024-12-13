from datetime import datetime, timedelta
from mexc_sdk import Spot

def find_trading_window(spot: Spot, symbol: str, end_date: datetime, too_far_back: datetime):
    interval = '1M'
    earliest_trading_month = None
    latest_trading_date = None

    while end_date > too_far_back:
        start_date = datetime(end_date.year, end_date.month, 1)

        klines = spot.klines(
            symbol=symbol,
            interval=interval,
            options={
                "startTime": int(start_date.timestamp() * 1000),
                "endTime": int(end_date.timestamp() * 1000),
                "limit": 1
            }            
        )

        if klines:
            # Update the latest trading data if it's the first data found
            if not latest_trading_date:
                latest_trading_date = end_date
            
            end_date = start_date - timedelta(days=1)
        else:
            earliest_trading_month = datetime(start_date.year + (start_date.month // 12), (start_date.month % 12) + 1, 1)
            break
    
    return {
        "symbol": symbol,
        "earliest trading month": earliest_trading_month, #.strftime("%Y-%m-%d %H:%M:%S") if earliest_missing_date else None,
        "latest trading date": latest_trading_date #.strftime("%Y-%m-%d %H:%M:%S") if latest_trading_date else None
    }

def find_first_trading_date(spot: Spot, symbol: str, earliest_trading_month: datetime):
    interval = '1d'
    end_date = datetime(earliest_trading_month.year + (earliest_trading_month.month // 12), (earliest_trading_month.month % 12) + 1, 1)

    while earliest_trading_month < end_date:
        klines = spot.klines(
            symbol=symbol,
            interval=interval,
            options={
                "startTime": int(earliest_trading_month.timestamp() * 1000),
                "endTime": int((earliest_trading_month + timedelta(days=1)).timestamp() * 1000),
                "limit": 1
            }
        )

        if klines:
            return datetime.fromtimestamp(klines[0][0] / 1000)
        else:
            earliest_trading_month = earliest_trading_month + timedelta(days=1)

def process_pair(spot: Spot, pair: str, current_date: datetime, too_far_back: datetime):
    symbol_info = find_trading_window(spot, pair, end_date=current_date, too_far_back=too_far_back)
    
    if symbol_info["earliest trading month"] and symbol_info["latest trading date"]:
        symbol_info["earliest trading date"] = find_first_trading_date(
            spot, 
            symbol=symbol_info["symbol"], 
            earliest_trading_month=symbol_info["earliest trading month"]
        )
    else:
        symbol_info["earliest trading date"] = None
    
    return symbol_info