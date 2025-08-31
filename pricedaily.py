import pandas as pd
import akshare as ak
from tqdm import tqdm
from datetime import datetime
import time
import warnings
from tables import NaturalNameWarning
from requests.exceptions import ConnectionError

import config
import profile

warnings.filterwarnings("ignore", category=NaturalNameWarning)

price_daily_df_field_mapping = {
    "日期": {"rename": "trade_date", "dtype": "datetime64[ns]"},  # 交易日
    "股票代码": {"rename": "stock_code", "dtype": "object"},  # 不带市场标识的股票代码
    "开盘": {"rename": "open_price", "dtype": "float64"},
    "收盘": {"rename": "close_price", "dtype": "float64"},
    "最高": {"rename": "high_price", "dtype": "float64"},
    "最低": {"rename": "low_price", "dtype": "float64"},
    "成交量": {"rename": "volume", "dtype": "int64"},  # 单位: 手
    "成交额": {"rename": "amount", "dtype": "float64"},  # 单位: 元
    "振幅": {"rename": "amplitude_pct", "dtype": "float64"},  # 单位: %
    "涨跌幅": {"rename": "change_pct", "dtype": "float64"},  # 单位: %
    "涨跌额": {"rename": "change_amount", "dtype": "float64"},  # 单位: 元
    "换手率": {"rename": "turnover_rate", "dtype": "float64"}  # 单位: %
}


def update_price_daily(adjust="qfq"):
    """
    :param adjust: 默认返回不复权的数据; qfq: 返回前复权后的数据; hfq: 返回后复权后的数据
    :return:
    """
    name_df = profile.get_stock_universe()
    symbols = name_df['Symbol'].to_list()

    now_str = datetime.now().strftime("%Y%m%d")

    is_skip = True
    start_symbol = "600694"

    for symbol in tqdm(symbols, desc="更新个股日线历史行情中"):
        if start_symbol:
            if is_skip:
                if symbol != start_symbol:
                    continue
                else:
                    is_skip = False

        while True:
            try:
                single_hist_df = ak.stock_zh_a_hist(
                    symbol=symbol,
                    period="daily",
                    start_date="20240101",
                    end_date=now_str,
                    adjust=adjust
                )
                break

            except:
                print(f"- 下载股票代码{symbol}时发生ConnectionError，将在1分钟后继续")
                time.sleep(60)

        single_hist_df = single_hist_df.rename(
            columns={k: v["rename"] for k, v in price_daily_df_field_mapping.items()}
        ).astype({v["rename"]: v["dtype"] for k, v in price_daily_df_field_mapping.items()})

        with pd.HDFStore(config.DATA_FILE, mode='a') as store:
            store.put(key=f'/price/daily/{symbol}', value=single_hist_df, format='table')

        time.sleep(1)


if __name__ == '__main__':
    update_price_daily()
