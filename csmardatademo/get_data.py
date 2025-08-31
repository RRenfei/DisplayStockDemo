import os
import requests
import pandas as pd

from csmardatademo.config import DAILY_PRICE_DATA_URL, WEEKLY_PRICE_DATA_URL, DATA_DATE_STR
from csmardatademo.process import PRICE_FIELD_MAPPING


def download_from_oss(local_path: str, url: str):
    """
    如果本地不存在文件，就从 OSS 下载
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    if not os.path.exists(local_path):
        print(f"Downloading {url} ...")
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Saved to {local_path}")
    return local_path


class GetProcessedCSMARData:
    def __init__(self, freq="weekly"):
        # 获取当前文件所在目录
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # 本地缓存目录（Streamlit Cloud 里也会用这个）
        data_dir = os.path.join(base_dir, "data", DATA_DATE_STR)

        # 定义 OSS URL
        if freq == "weekly":
            self.price_hdf = os.path.join(data_dir, f"{DATA_DATE_STR}_week.hdf")
            self.price_url = WEEKLY_PRICE_DATA_URL
        elif freq == "daily":
            self.price_hdf = os.path.join(data_dir, f"{DATA_DATE_STR}.hdf")
            self.price_url = DAILY_PRICE_DATA_URL
        else:
            raise ValueError(f"freq取值为 'weekly' 或 'daily'，不可以是 '{freq}'")

        # 确保文件存在（没有就下载）
        download_from_oss(self.price_hdf, self.price_url)

    def price(self, symbol) -> pd.DataFrame:
        try:
            return pd.DataFrame(pd.read_hdf(self.price_hdf, key=symbol))
        except KeyError:
            return pd.DataFrame(columns=[v["rename"] for v in PRICE_FIELD_MAPPING.values()])


if __name__ == "__main__":
    # 测试
    df = GetProcessedCSMARData(freq="weekly").price(symbol="000001")
    print(df.head())
