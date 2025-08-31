import os
import pandas as pd
import akshare as ak

import config


def update_stock_universe(write_data=True):
    name_df_dtypes = {
        'Symbol': 'str',  # 证券代码 (字符串)
        'ShortName': 'str',  # 证券简称 (字符串)
        'ListingDate': 'datetime64[ns]'  # 上市日期 (日期类型)
    }

    sh_name_df = ak.stock_info_sh_name_code(symbol="主板A股")
    sh_name_df = sh_name_df[['证券代码', '证券简称', '上市日期']].rename(columns={
        '证券代码': 'Symbol',
        '证券简称': 'ShortName',
        '上市日期': 'ListingDate',
    })
    print(f'OK 上海证券交易所股票列表获取完毕，共有股票{len(sh_name_df)}个')

    sz_name_df = ak.stock_info_sz_name_code(symbol="A股列表")
    sz_name_df = sz_name_df[['A股代码', 'A股简称', 'A股上市日期']].rename(columns={
        'A股代码': 'Symbol',
        'A股简称': 'ShortName',
        'A股上市日期': 'ListingDate',
    })
    print(f'OK 深圳证券交易所股票列表获取完毕，共有股票{len(sz_name_df)}个')

    bj_name_df = ak.stock_info_bj_name_code()
    bj_name_df = bj_name_df[['证券代码', '证券简称', '上市日期']].rename(columns={
        '证券代码': 'Symbol',
        '证券简称': 'ShortName',
        '上市日期': 'ListingDate',
    })
    print(f'OK 北京证券交易所股票列表获取完毕，共有股票{len(bj_name_df)}个')

    name_df = pd.concat([sh_name_df, sz_name_df, bj_name_df], ignore_index=True)
    name_df = name_df.astype(name_df_dtypes)

    if write_data:
        with pd.HDFStore(config.DATA_FILE, mode='w') as store:
            store.put(key=config.DATA_KEY_PROFILE, value=name_df, format='table')

    print('OK 所有股票列表数据合并完毕')

    return name_df


def get_stock_universe():
    # 检查文件是否存在
    if not os.path.exists(config.DATA_FILE):
        print(f"- 数据文件{config.DATA_FILE}不存在，将执行update_stock_universe更新股票列表")
        update_stock_universe(write_data=True)
    else:
        with pd.HDFStore(config.DATA_FILE, mode='r') as store:
            if config.DATA_KEY_PROFILE not in store:
                print(f"- 数据文件{config.DATA_FILE}中不存在键{config.DATA_KEY_PROFILE}，将执行update_stock_universe更新股票列表")
                update_stock_universe(write_data=True)

    with pd.HDFStore(config.DATA_FILE, mode='r') as store:
        return store[config.DATA_KEY_PROFILE]


if __name__ == '__main__':
    update_stock_universe()
