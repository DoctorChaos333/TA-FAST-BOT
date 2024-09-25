import asyncio
import json
import os
from dotenv import load_dotenv, find_dotenv
import datetime
import aiomysql
from aiomysql.cursors import DictCursor
import contextvars
import random
import string
import subprocess
import aiofiles
import cryptography
import pymysql
import math
import traceback

import pandas as pd
df = pd.read_excel('База предметов.xlsx')
result_dict = dict(zip(df['Предмет'], df['Какой float ценится']))
for k in result_dict:
    v = result_dict[k]
    v1 = [[float(num) for num in item.split('-')] for item in v.split(' | ')]
    result_dict[k] = v1

def is_rarity(skin, fl, percent):
    fl = float(fl)

    percent = float(percent)
    if percent > -10:
        return True
    if result_dict.get(skin):
        for item in result_dict[skin]:
            if item[0] <= fl <= item[1]:
                return True
    if fl == 1:
        return True

    return False

dotenv_path = "C:\\Users\\Administrator\\Desktop\\TA-FAST-BOT-main\\.env"
load_dotenv(dotenv_path = "C:\\Users\\Administrator\\Desktop\\TA-FAST-BOT-main\\.env")

class Storage:
    connection = contextvars.ContextVar('connection')

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()

    async def init_db(self):
        host = os.getenv('DB_HOST')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        db_name = os.getenv('DB_DATABASE')
        port = int(os.getenv('DB_PORT'))

        pool = await aiomysql.create_pool(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db_name,
            cursorclass=DictCursor,
            loop=self.loop
        )
        Storage.connection.set(pool)

    async def close(self):
        pool = Storage.connection.get()
        pool.close()
        await pool.wait_closed()

    async def __aenter__(self):
        await self.init_db()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def execute(self, query, args=None):
        pool = Storage.connection.get()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Если args переданы, выполняем запрос с параметрами
                if args:
                    await cur.execute(query, args)
                else:
                    await cur.execute(query)
                await conn.commit()

    async def executemany(self, query, args=None):
        pool = Storage.connection.get()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Если args переданы, выполняем запрос с параметрами
                if args:
                    await cur.executemany(query, args)
                else:
                    await cur.execute(query)
                await conn.commit()
                

    async def fetchall(self, query, *args, **kwargs):
        pool = Storage.connection.get()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, *args, **kwargs)
                return await cur.fetchall()

    async def fetchone(self, query, *args, **kwargs):
        pool = Storage.connection.get()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, *args, **kwargs)
                return await cur.fetchone()

    async def smth(self, buy_id, skin, id_, listing_price, default_price, steam_without_fee, sticker, url, ts, wear, sticker_slot, sticker_price, profit, percent, fl):
        sticker = str(sticker)
        query = """INSERT IGNORE INTO `spam_profit_temp` (buy_id, skin, id, listing_price, default_price, steam_without_fee, sticker, url, ts, wear, sticker_slot, sticker_price, profit, percent, market_actions, fl) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        await self.execute(query, (str(buy_id), str(skin), str(id_), str(listing_price), str(default_price), str(steam_without_fee), str(sticker), str(url), ts, str(wear), str(sticker_slot), str(sticker_price), str(profit), str(percent), str(fl)))

    async def add_sticker(self, skin, default_price, ts):
        query = "INSERT INTO spam_stickers (skin, default_price, ts) VALUES (%s, %s, %s) AS alias ON DUPLICATE KEY UPDATE default_price = alias.default_price, ts = alias.ts"
        await self.execute(query, (str(skin), str(default_price), str(ts)))


    async def smthmany(self, args: list, name='spam_profit_temp'):
        if name == 'spam_profit_temp':
            query = """INSERT INTO `spam_profit_temp` (buy_id, skin, id, listing_price, default_price, steam_without_fee, sticker, url, ts, wear, sticker_slot, sticker_price, profit, percent, market_actions, fl, page_num) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE profit=IF(profit > 0, VALUES(profit), profit), percent=IF(percent > 0, VALUES(percent), percent), ts = ts"""
            await self.executemany(query, args)
        elif name == 'spam_profit':

            if args:
                query = """INSERT IGNORE INTO `spam_profit` (buy_id, skin, id, listing_price, default_price, steam_without_fee, sticker, url, ts, wear, sticker_slot, sticker_price, profit, percent, fl, page_num) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                ids = [arg[2] for arg in args]
                print(f'Добавляю id {ids} в базу флотов')
                filtered_args = []
                for arg in args:
                    if is_rarity(arg[1], arg[-2], arg[-3]):
                        filtered_args.append(arg)
                if filtered_args:
                    for arg in filtered_args:
                        lst_arg = list(arg)
                        await asyncio.sleep(1.1)
                        lst_arg[-8] = datetime.datetime.now()
                        arg = tuple(lst_arg)
                        await self.execute(query, arg)
                for i in filtered_args:
                    print('Добавляю', i, 'в spam_profit')
                ids_str = ",".join(str(id) for id in ids)
                query = "DELETE FROM spam_profit_temp WHERE id IN ({})".format(ids_str)
                await self.execute(query)

                query = F"INSERT INTO spam_float (id, fl, wear, ts) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE wear = VALUES(wear)"
                float_args = [(item[2], item[-2], item[-7], item[-8]) for item in args]
                await self.executemany(query, float_args)

    async def is_float_exists(self, buy_id):
        query = "SELECT `fl` FROM `spam_profit_temp` WHERE buy_id = %s"
        rs = await self.fetchone(query, (buy_id))
        if isinstance(rs, dict) and rs.get('fl'):
            return rs.get('fl')
        else:
            return None

    async def fetch_all_stickers(self):
        query = "SELECT `skin`, `ts`, `default_price` FROM `spam_stickers`"
        stickers_dct = await self.fetchall(query)
        rs = dict()
        for st in stickers_dct:
            if datetime.datetime.now() - st['ts'] < datetime.timedelta(hours=24) and st['skin'] not in rs:
                rs[st['skin']] = float(st['default_price'])
        return rs
    
    async def is_sticker_exists(self, sticker):
        query = "SELECT `default_price` FROM `spam_stickers` WHERE skin = %s"
        rs = await self.fetchone(query, (sticker))
        if isinstance(rs, dict) and rs.get('default_price'):
            return str(rs.get('default_price'))
        else:
            return None

    async def fetch_sticker_price(self, sticker):
        query = "SELECT `default_price` FROM `spam_profit_temp` WHERE skin = %s"
        rs = await self.fetchone(query, (sticker))
        if isinstance(rs, dict) and rs.get('default_price'):
            return str(rs.get('default_price'))
        else:
            return -1

    async def fetch_temp(self):
        # query = "SELECT * FROM spam_profit_temp WHERE id NOT IN (SELECT id FROM spam_profit)"
        query = "DELETE FROM spam_profit WHERE ts < NOW() - INTERVAL 7 HOUR;"
        await self.execute(query)
        query = "DELETE FROM spam_profit_temp WHERE ts < NOW() - INTERVAL 7 HOUR;"
        await self.execute(query)
        print('Удалил все старые предметы из spam_profit')
        query = "SELECT * FROM spam_profit_temp"
        rs = await self.fetchall(query)
        rs = sorted(rs, key=lambda x: x['profit'], reverse=True)
        #print(len(rs))
        if rs:
            query = "SELECT * FROM spam_float WHERE id IN (SELECT id FROM spam_profit_temp) AND ts >= NOW() - INTERVAL 7 DAY"
            floats = await self.fetchall(query)
            floats_dct = {item['id']: {'fl': item['fl'], 'wear': item['wear']} for item in floats}
            for item in rs:
                if item['id'] in floats_dct:
                    print(floats_dct[item['id']])
                    item['fl'] = floats_dct[item['id']]['fl']
                    item['wear'] = floats_dct[item['id']]['wear']
            rs = list(filter(lambda x: is_rarity(x['skin'], float(x['fl']), x['profit']), rs))
        if len(rs) > 1000:
            return rs[:1000]
        return rs

    async def update_hunting_temp(self, id_, wear, fl, account_id, buy_id, price, default_price, sticker='float', page_num=0, is_hunting=0, fee=0):
        query = "UPDATE spam_hunting_temp SET fl = %s, wear = %s WHERE id = %s"
        try:
            await self.execute(query, (fl, wear, id_))
        except:
            print(traceback.format_exc())
        ts = datetime.datetime.now()
        print(f'Добавил в базу флотов {id_}')
        query = F"INSERT INTO spam_float (id, fl, wear, ts) VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE wear = VALUES(wear), ts = VALUES(ts)"
        await self.execute(query, (id_, fl, wear, ts))

        info = await self.get_hunting_float(account_id)
        needed_float_value = info['float_value']
        login_to_buy = info['login_to_buy']
        skin = info['skin']
        bot_price = round(price * 0.87, 2)

        min_value, max_value = list(map(float, needed_float_value.replace(',', '.').split('-')))
        if min_value < float(fl) < max_value:
            query = "INSERT IGNORE INTO spam_ticket (steam_login, skin, buy_id, price, bot_price, default_price, float_value, sticker, page_num, is_hunting, fee) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            await self.execute(query, (login_to_buy, skin, buy_id, price, bot_price, default_price, fl, sticker, page_num, is_hunting, fee))       

    async def fetch_hunt_temp(self):
        query = "SELECT * FROM spam_hunting_temp WHERE fl = 1"
        items = await self.fetchall(query)

        if len(items) > 1000:
            items = items[:1000]

        items_dict = dict()
        for item in items:
            if item['account_id'] not in items_dict:
                items_dict[item['account_id']] = []
            items_dict[item['account_id']].append(
                {
                    'id': item['id'],
                    'fl': item['fl'],
                    'wear': item['wear'],
                    'sticker': item['sticker'],
                    'market_actions': item['market_actions'],
                    'default_price': float(item['default_price']),
                    'listing_price': float(item['listing_price']),
                    'account_id': item['account_id'],
                    'buy_id': item['buy_id'],
                    'page_num': item['page_num'],
                    'fee': item['fee']
                }
            )
        return items_dict
    
    async def get_hunting_float(self, account_id):
        query = "SELECT * FROM spam_hunting WHERE id = %s"
        info = await self.fetchone(query, account_id)
        if info :
            return info
    async def add_to_spam_hunting_temp(self, args: tuple):
        query = "INSERT IGNORE INTO spam_hunting_temp (id, fl, market_actions, listing_price, default_price, sticker, wear, account_id, buy_id, ts, page_num, fee) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        await self.executemany(query, args)

    async def get_hunting_info(self):
        query = "SELECT * FROM spam_hunting WHERE is_active = 1"
        info = await self.fetchall(query)
        return info
    async def get_all_floats(self):
        query = "SELECT * FROM spam_float"
        floats = await self.fetchall(query)
        floats = {item['id']: item['fl'] for item in floats}
        return floats
    async def fetch_hunting_proxies(self, ip=6):
        if ip == 4:
            query = "SELECT proxy FROM spam_hunting_proxy WHERE ip = 4"
        else:
            query = "SELECT proxy FROM spam_hunting_proxy"
        proxies = ['http://' + item['proxy'].strip().split(':')[2] + ':' + item['proxy'].strip().split(':')[3] + '@' +
                        item['proxy'].strip().split(':')[
                            0] + ':' + item['proxy'].strip().split(':')[1] for item in await self.fetchall(query)]
        return proxies

    async def check_new_tickets(self):
        query = "SELECT * FROM spam_ticket WHERE completed = 0"
        return await self.fetchall(query)

    async def get_spam_users(self):
        query = "SELECT login, password, steamID, shared, identity, session, API, last_session_ts, currency FROM spam_users WHERE is_active = 1"
        return await self.fetchall(query)

    async def get_spam_user(self, login):
        query = "SELECT login, password, steamID, shared, identity, session, API, last_session_ts, currency FROM spam_users WHERE is_active = 1 AND login = %s"
        return await self.fetchone(query, (login))

    async def update_session(self, login: str, session: bytes, ts):
        query = "UPDATE spam_users SET session = %s, last_session_ts = %s WHERE login = %s"
        # Проверка на NULL для session
        if session is not None:
            await self.execute(query, (session, ts, login))
        else:
            query = "UPDATE spam_users SET last_session_ts = %s WHERE login = %s"
            await self.execute(query, (ts, login))

    async def update_ticket_status(self, buy_id: str, status: int):
        ts = datetime.datetime.now()
        if status == 1:
            query = "UPDATE spam_ticket SET success = 1, completed = 1, ts_attempt = %s WHERE buy_id = %s"
        else:
            query = "UPDATE spam_ticket SET already = 1, completed = 1, ts_attempt = %s WHERE buy_id = %s"
        await self.execute(query, (ts, buy_id))
    async def get_all_floats_from_temp(self):
        query = "SELECT id FROM spam_hunting_temp"
        floats = await self.fetchall(query)
        floats = [item['id'] for item in floats]
        return floats
    async def get_default_price(self, account_id):
        query = "SELECT default_price, fee FROM spam_hunting_temp WHERE ts > NOW() - INTERVAL 5 HOUR AND account_id = %s"
        default_prices = await self.fetchall(query, (account_id))
        if default_prices:
            default_price_and_fee = min([(item['default_price'], item['fee']) for item in default_prices], key = lambda x: x[0])
            return default_price_and_fee
        else:
            return (math.inf, math.inf)
