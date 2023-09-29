import aiohttp
import asyncio
import logging
import json
import re
import aiomysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

CONCURRENCY = 50
semaphore = asyncio.Semaphore(CONCURRENCY)
session = None

TOTAL_url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112305777638389464645_1695217203836&sortColumns=PLAN_NOTICE_DATE&sortTypes=-1&pageSize=50&pageNumber={page}&reportName=RPT_SHAREBONUS_DET&columns=ALL&quoteColumns=&js=%7B%22data%22%3A(x)%2C%22pages%22%3A(tp)%7D&source=WEB&client=WEB&filter=(REPORT_DATE%3D%27{date}%27)"

async def create_pool():
    pool = await aiomysql.create_pool(host='localhost', user='root', password='289941', db='spiders')
    return pool

async def create_table(conn):
    async with conn.cursor() as cur:
        sql_create = 'CREATE TABLE IF NOT EXISTS stock_table_2 (id INT AUTO_INCREMENT PRIMARY KEY, code CHAR(6) NOT NULL, local CHAR(2),\
                        name VARCHAR(10), date CHAR(4), plan_date DATE, BONUS_IT_RATIO FLOAT, BONUS_RATIO FLOAT, IT_RATIO FLOAT,\
                        PRETAX_BONUS_RMB FLOAT, DIVIDENT_RATIO FLOAT, BASIC_EPS FLOAT, BVPS FLOAT, PER_CAPITAL_RESERVE FLOAT,\
                        PER_UNASSIGN_PROFIT FLOAT, PNP_YOY_RATIO FLOAT)'
        try:
            await cur.execute(sql_create)
            await conn.commit()
        except Exception as e:
            logging.info('创建表格时出错: %s',e)
        logging.info('创建表格完毕')
        

async def get_date():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112307096466128394474_1695712620155&reportName=RPT_DATE_SHAREBONUS_DET&columns=ALL&quoteColumns=&pageNumber=1&sortColumns=REPORT_DATE&sortTypes=-1&source=WEB&client=WEB&_=1695712620156"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html = await response.text()
    a = re.findall("\((.*)\)", html, re.DOTALL)
    b = json.loads(a[0])
    date_list = []
    for item in b['result']['data']:
        date = item['REPORT_DATE']
        date_list.append(date[:10])
    return date_list

async def scrape_api(url):
    async with semaphore:
        try:
            logging.info('正在获取 %s', url)
            async with session.get(url) as response:
                a = await response.text()
                b = re.findall("\((.*)\)", a, re.DOTALL)
                #print(type(b[0]))
                c = json.loads(b[0])
                #print(type(c))
                return c
        except aiohttp.ClientError as e:
            logging.error('获取数据时发生错误 %s: %s', url, e)

async def process_data(data_results,conn):
    a = 0
    for data in data_results:
        for i in range(len(data['result']["data"])):
            b = data['result']["data"][i]
            #提供一个元组，传入sql语言
            #id,股票代码,上市地区,公司名称,分红年份,公告日期,
            #送转股总比例,
            #送股比例,
            #转股比例,
            #现金分红比例,
            #股息率,每股收益,每股净资产,每股公积金,
            #每股未分配利润,净利润同比增长率
            a += 1
            value = (b['SECUCODE'][:6],b['SECUCODE'][-2:],b["SECURITY_NAME_ABBR"],b["NOTICE_DATE"][:4],b["PLAN_NOTICE_DATE"],
                    int(b["BONUS_IT_RATIO"])/10 if b["BONUS_IT_RATIO"] is not None else b["BONUS_IT_RATIO"],
                    int(b["BONUS_RATIO"])/10 if b["BONUS_RATIO"] is not None else b["BONUS_RATIO"],
                    int(b["IT_RATIO"])/10 if b["IT_RATIO"] is not None else b["IT_RATIO"],
                    int(b["PRETAX_BONUS_RMB"])/10 if b["PRETAX_BONUS_RMB"] is not None else b["PRETAX_BONUS_RMB"],
                    b['DIVIDENT_RATIO'],b["BASIC_EPS"],b["BVPS"],b["PER_CAPITAL_RESERVE"],
                    b["PER_UNASSIGN_PROFIT"],b["PNP_YOY_RATIO"])
            #logging.info('数据处理完成 %s', value)
            if a % 100 == 0:
                logging.info('数据处理 +100 条数据')
            async with conn.cursor() as cur:
                sql ='INSERT INTO stock_table_2 (code, local, name, date, plan_date, BONUS_IT_RATIO, BONUS_RATIO, IT_RATIO,\
                    PRETAX_BONUS_RMB, DIVIDENT_RATIO, BASIC_EPS, BVPS, PER_CAPITAL_RESERVE, PER_UNASSIGN_PROFIT, PNP_YOY_RATIO)\
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                try:
                    await cur.execute(sql, value)
                    await conn.commit()
                except Exception as e:
                    logging.error('插入数据时出错：%s', e)

async def get_page(date):
    page = 1
    url = TOTAL_url.format(page=page, date=date)
    data = await scrape_api(url)
    page = data['result']['pages']
    return date,page

async def main():
    global session
    date_list = await get_date()
    pool = await create_pool()
    conn = await pool.acquire()
    session = aiohttp.ClientSession()
    page_tasks = [get_page(date) for date in date_list]
    data_tasks = []
    page_results = await asyncio.gather(*page_tasks)
    for date, page in page_results:
        for i in range(1, page + 1):
            url = TOTAL_url.format(page=i, date=date)
            data_tasks.append(scrape_api(url))
        data_results = await asyncio.gather(*data_tasks)
        await process_data(data_results, conn)
        data_tasks = []

if __name__ == "__main__":
    asyncio.run(main())