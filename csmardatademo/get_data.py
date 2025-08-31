import os
import requests
import pandas as pd
import gdown

from csmardatademo.config import DAILY_PRICE_DATA_URL, WEEKLY_PRICE_DATA_URL
from csmardatademo.process import PRICE_FIELD_MAPPING


def download_from_gdown(local_path: str, url: str):
    """
    如果本地不存在文件，就从 OSS 下载
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    if not os.path.exists(local_path):
        print(f"Downloading {url} ...")
        gdown.download(url, local_path, quiet=False)
        print(f"Saved to {local_path}")
    return local_path


class GetProcessedCSMARData:
    def __init__(self, freq="weekly"):
        # 获取当前文件所在目录
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # 本地缓存目录
        data_dir = os.path.join(base_dir, "data")

        # 定义 OSS URL
        if freq == "weekly":
            self.price_hdf = os.path.join(data_dir, f"weekly_price.h5")
            self.price_url = WEEKLY_PRICE_DATA_URL
        elif freq == "daily":
            self.price_hdf = os.path.join(data_dir, f"daily_price.h5")
            self.price_url = DAILY_PRICE_DATA_URL
        else:
            raise ValueError(f"freq取值为 'weekly' 或 'daily'，不可以是 '{freq}'")

        # 确保文件存在（没有就下载）
        download_from_gdown(self.price_hdf, self.price_url)

    def price(self, symbol) -> pd.DataFrame:
        try:
            return pd.DataFrame(pd.read_hdf(self.price_hdf, key=symbol))
        except KeyError:
            return pd.DataFrame(columns=[v["rename"] for v in PRICE_FIELD_MAPPING.values()])


if __name__ == "__main__":
    # 测试
    df = GetProcessedCSMARData(freq="weekly").price(symbol="000001")
    print(df.head())
