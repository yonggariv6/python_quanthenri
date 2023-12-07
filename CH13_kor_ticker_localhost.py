import requests as rq
from bs4 import BeautifulSoup

url = 'https://finance.naver.com/sise/sise_deposit.naver'
data = rq.get(url)
data_html = BeautifulSoup(data.content)

parse_day = data_html.select_one(
'div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text

parse_day

import re

biz_day = re.findall('[0-9]+', parse_day)
biz_day = ''.join(biz_day)
biz_day

from io import BytesIO
import pandas as pd

# 코스피(STK)
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_stk = {
    'mktId' : 'STK',
    'trdDd' : biz_day,
    'money' : 1,
    'csvxls_isNo' : 'false',
    'name' : 'fileDown',
    'url' : 'dbms/MDC/STAT/standard/MDCSTAT03901'
}

headers = {'Referer' : 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_stk = rq.post(down_url, {'code' : otp_stk}, headers=headers)

sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding = 'EUC-KR')

# 코스닥(KSQ)
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_ksq = {
    'mktId' : 'KSQ',
    'trdDd' : biz_day,
    'money' : 1,
    'csvxls_isNo' : 'false',
    'name' : 'fileDown',
    'url' : 'dbms/MDC/STAT/standard/MDCSTAT03901'
}

headers = {'Referer' : 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
otp_ksq = rq.post(gen_otp_url, gen_otp_ksq, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_ksq = rq.post(down_url, {'code' : otp_ksq}, headers=headers)

sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding = 'EUC-KR')

krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop = True)
krx_sector['종목명'] = krx_sector['종목명'].str.strip()
krx_sector['기준일'] = biz_day

# 개별종목
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_all = {
    'mktId' : 'ALL',
    'trdDd' : biz_day,
    'csvxls_isNo' : 'false',
    'name' : 'fileDown',
    'url' : 'dbms/MDC/STAT/standard/MDCSTAT03501'    
}

headers = {'Referer' : 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
otp_all = rq.post(gen_otp_url, gen_otp_all, headers=headers).text

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
krx_ind = rq.post(down_url, {'code' : otp_all}, headers=headers)


krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding = 'EUC-KR')
krx_ind['종목명'] = krx_ind['종목명'].str.strip()
krx_ind['기준일'] = biz_day

set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명']))

kor_ticker = pd.merge(
    krx_sector,
    krx_ind,
    on = krx_sector.columns.intersection(
        krx_ind.columns).tolist(),
    how='outer')
 
#스팩주
kor_ticker[kor_ticker['종목명'].str.contains('스펙|제[0-9]+호')]['종목명']

#우선주 : 종목코드 끝자리가 '0'
kor_ticker[kor_ticker['종목코드'].str[-1:] != '0']['종목명']

#리츠
kor_ticker[kor_ticker['종목명'].str.endswith('리츠')]['종목명']


import numpy as np

diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])))

kor_ticker['종목구분'] = np.where(kor_ticker['종목명'].str.contains('스펙|제(0-9)+호'), '스펙',
        np.where(kor_ticker['종목코드'].str[-1:] != '0', '우선주',
        np.where(kor_ticker['종목명'].str.endswith('리츠'), '리츠',
        np.where(kor_ticker['종목명'].isin(diff), '기타',
        '보통주'))))

kor_ticker = kor_ticker.reset_index(drop = True)
kor_ticker.columns = kor_ticker.columns.str.replace(' ','')
kor_ticker = kor_ticker[['종목코드', '종목명', '시장구분', '종가', '시가총액', '기준일', 'EPS', '선행EPS', 'BPS', '주당배당금', '종목구분']]
kor_ticker = kor_ticker.replace({np.nan: None})

import pymysql

con = pymysql.connect(
    user = 'root',
    passwd = '7963',
    host = '127.0.0.1',
    db = 'stock_db',
    charset = 'utf8'
)

mycursor = con.cursor()

query = f"""
    insert into kor_ticker (종목코드,종목명,시장구분,종가,시가총액,기준일,EPS,선행EPS,BPS,주당배당금,종목구분)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) as new
    on duplicate key update 
    종목명 = new.종목명, 시장구분 = new.시장구분, 종가 = new.종가, 시가총액 = new.시가총액, 기준일 = new.기준일,
    EPS = new.EPS, 선행EPS = new.선행EPS, BPS = new.BPS, 주당배당금 = new.주당배당금, 종목구분 = new.종목구분;
"""

args = kor_ticker.values.tolist()
mycursor.executemany(query, args)
con.commit()

con.close()
   