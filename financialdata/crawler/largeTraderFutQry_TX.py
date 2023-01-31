import os
from datetime import datetime
import sys
import time
import typing

import pandas as pd
import requests
from loguru import logger
from pydantic import BaseModel
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import configparser

env_path = os.path.join(os.path.expanduser('~'), 'env')

def get_db_config():
    config = configparser.ConfigParser()
    config.read(os.path.join(env_path, 'db_config.ini'))
    return config

def futures_header():
    """網頁瀏覽時, 所帶的 request header 參數, 模仿瀏覽器發送 request"""
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Length": "101",
        "Content-Type": "application/x-www-form-urlencoded",
        "Host": "www.taifex.com.tw",
        "Origin": "https://www.taifex.com.tw",
        "Pragma": "no-cache",
        "Referer": "https://www.taifex.com.tw/cht/3/dlFutDailyMarketView",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
    }

def crawler_futures(date: str, prod:str='TX') -> pd.DataFrame:
    """期交所爬蟲"""
    url = "https://www.taifex.com.tw/cht/3/largeTraderFutQry"
    form_data = {
        "datecount": "",
        "contractId2": "",
        "queryDate": date.replace("-", "/"),
        "contractId": prod,
    }
    # 避免被期交所 ban ip, 在每次爬蟲時, 先 sleep 5 秒
    time.sleep(10)
    resp = requests.post(
        url,
        headers=futures_header(),
        data=form_data,
    )
    if resp.ok:
        Soup = BeautifulSoup(resp.content,'html.parser') 
        if Soup.find(class_='table_f') != None:
            table_f = Soup.find(class_='table_f')
            all_contracts = table_f("tr")[-1]
            buyer_a_01_01 = all_contracts.find(headers ='buyer_a_01_01').text
            buyer_a_01_01_index_1 = buyer_a_01_01.index('(')
            buyer_a_01_01_index_2 = buyer_a_01_01.index(')')
            buyer_top5 = int(buyer_a_01_01[(buyer_a_01_01_index_1+1):buyer_a_01_01_index_2].replace(",", ""))

            seller_a_01_01 = all_contracts.find(headers ='seller_a_01_01').text
            seller_a_01_01_index_1 = seller_a_01_01.index('(')
            seller_a_01_01_index_2 = seller_a_01_01.index(')')
            seller_top5 = int(seller_a_01_01[(seller_a_01_01_index_1+1):seller_a_01_01_index_2].replace(",", ""))

            datetime_now = datetime.now().isoformat(sep=" ", timespec="seconds")
            d = {'date': [date], 
                 'prod': [prod],
                 'buyer_top5': [buyer_top5],
                 'seller_top5': [seller_top5],
                 'create_date': [datetime_now],
                 'modify_date': [datetime_now]}
            df = pd.DataFrame(data=d)

        else:
            logger.info("no data")
            return pd.DataFrame()

    else:
        logger.info("request failed")
        return pd.DataFrame()
    return df

class TaiwanFuturesDaily(BaseModel):
    date: str
    prod: str
    buyer_top5: int
    seller_top5: int
    create_date: datetime
    modify_date: datetime

def check_schema(df: pd.DataFrame) -> pd.DataFrame:
    """檢查資料型態, 確保每次要上傳資料庫前, 型態正確"""
    df_dict = df.to_dict("records")
    df_schema = [
        TaiwanFuturesDaily(
            **dd
        ).__dict__
        for dd in df_dict
    ]
    df = pd.DataFrame(df_schema)
    return df

def gen_date_list(start_date: str, end_date: str) -> typing.List[str]:
    """建立時間列表, 用於爬取所有資料"""
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    date_list = pd.date_range(start_date, end_date).strftime('%Y-%m-%d')

    return date_list

def main(start_date: str, end_date: str):
    date_list = gen_date_list(start_date, end_date)
    for date in date_list:
        logger.info(date)
        df = crawler_futures(date)
        if len(df) > 0:
            # 檢查資料型態
            df = check_schema(df.copy())

            db_config = get_db_config()
            financial_db_info= db_config['prod']
            db = create_engine('postgresql://'+ financial_db_info['user'] +':'+ financial_db_info['password'] +'@'+ financial_db_info['host'] + ':5432/' + financial_db_info['dbname'])
            try:
                df.to_sql(name='large_trader_fut_qry', con=db, if_exists='append', index=False)
                logger.info("success insert data")
            except Exception as e:
                logger.info(e)
            db.dispose()

if __name__ == "__main__":
    start_date, end_date = sys.argv[1:]
    main(start_date, end_date)


