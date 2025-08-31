import os
import json
import pandas as pd
from tqdm import tqdm
from tables import NaturalNameWarning
import warnings

from csmardatademo.config import DATA_DATE_STR

warnings.filterwarnings("ignore", category=NaturalNameWarning)

# 当前文件夹路径和数据所在文件夹的绝对路径
base_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(base_dir, "data", str(DATA_DATE_STR))

PRICE_FIELD_MAPPING = {
    "TradingDate": {"rename": "trading_date", "dtype": "datetime64[ns]"},
    "Symbol": {"rename": "symbol", "dtype": "object"},
    "ShortName": {"rename": "short_name", "dtype": "object"},
    "OpenPrice": {"rename": "open_price", "dtype": "float64"},
    "ClosePrice": {"rename": "close_price", "dtype": "float64"},
    "HighPrice": {"rename": "high_price", "dtype": "float64"},
    "LowPrice": {"rename": "low_price", "dtype": "float64"},
    "Volume": {"rename": "volume", "dtype": "int64"},
    "Amount": {"rename": "amount", "dtype": "float64"},
    "ChangeRatio": {"rename": "change_ratio", "dtype": "float64"},

    # "Filling": {"rename": "filling", "dtype": "float64"},
    # "CirculatedShare": {"rename": "circulated_share", "dtype": "float64"},
    # "TotalShare": {"rename": "total_share", "dtype": "float64"},
}


def save_as_hdf():
    # 获取目录下所有CSV文件路径
    all_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".csv")]

    # 读取并合并所有CSV文件
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

    # 重命名字段
    df = df[PRICE_FIELD_MAPPING.keys()].rename(columns={k: v["rename"] for k, v in PRICE_FIELD_MAPPING.items()})

    # 将symbol转为字符串格式并用0将股票代码填充至6位
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)

    # 按照PRICE_FIELD_MAPPING强转换数据格式
    df = df.astype({v["rename"]: v["dtype"] for v in PRICE_FIELD_MAPPING.values()})

    # 按个股以此将日频数据保存至同路径的hdf文件
    symbols = df["symbol"].unique().tolist()
    hdf_name = f"{DATA_DATE_STR}.hdf"

    with pd.HDFStore(os.path.join(data_dir, hdf_name), mode='w') as store:
        for symbol in tqdm(symbols, desc=f"正在按个股保存数据至{hdf_name}"):
            df_symbol = df[df["symbol"] == symbol].sort_values("trading_date")
            store.put(key=symbol, value=df_symbol, format='table', complevel=9, complib='zlib')


class CSMARDataProcessor:
    def __init__(self):
        # 当前文件所在目录

        present2_dir = os.path.dirname(os.path.dirname(base_dir))  # 上两级目录

        self.daily_hdf = os.path.join(data_dir, f"{DATA_DATE_STR}.hdf")
        self.weekly_hdf = os.path.join(data_dir, f"{DATA_DATE_STR}_week.hdf")
        self.shortname_symbol_json = os.path.join(present2_dir, "shortname_symbol.json")

    def derive_weekly_prices(self):
        """日频数据 → 周频数据"""
        with pd.HDFStore(self.weekly_hdf, mode='w') as store:
            with pd.HDFStore(self.daily_hdf, mode='r') as reader:
                for key in tqdm(reader.keys(), desc="按个股，重采样为周线样本中"):
                    symbol = key.split('/')[-1]
                    df = reader[key]

                    # 确保日期格式正确，并排序
                    df['trading_date'] = pd.to_datetime(df['trading_date'])
                    df = df.sort_values('trading_date').set_index('trading_date')

                    # 重采样到周频
                    df_weekly = pd.DataFrame()
                    df_weekly['open_price'] = df['open_price'].resample('W-FRI').first()
                    df_weekly['close_price'] = df['close_price'].resample('W-FRI').last()
                    df_weekly['high_price'] = df['high_price'].resample('W-FRI').max()
                    df_weekly['low_price'] = df['low_price'].resample('W-FRI').min()
                    df_weekly['volume'] = df['volume'].resample('W-FRI').sum()
                    df_weekly['amount'] = df['amount'].resample('W-FRI').sum()
                    df_weekly['change_ratio'] = df['change_ratio'].resample('W-FRI').last()
                    df_weekly['short_name'] = df['short_name'].resample('W-FRI').last()
                    df_weekly['symbol'] = symbol
                    df_weekly['trading_date'] = df_weekly.index

                    # 删除 nan 行
                    df_weekly.dropna(subset=['open_price', 'close_price'], inplace=True)

                    # 重置索引
                    df_weekly.reset_index(drop=True, inplace=True)

                    # 保存周数据到 hdf 文件中
                    store.put(f"{symbol}", df_weekly, format='table', complevel=9, complib='zlib')

    def generate_shortname_symbol_json(self):
        """生成 {short_name: symbol} 的 JSON"""
        mapping = {}
        with pd.HDFStore(self.daily_hdf, mode='r') as reader:
            for key in tqdm(reader.keys(), desc="生成 short_name-symbol 映射中"):
                df = reader[key]
                if df.empty:
                    print(f"{key} 数据为空，跳过")
                    continue

                short_name = df.iloc[0]['short_name'].strip()
                symbol = df.iloc[0]['symbol'].strip()
                mapping[short_name] = symbol

        with open(self.shortname_symbol_json, "w", encoding="utf-8") as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    p = CSMARDataProcessor()
    p.derive_weekly_prices()
