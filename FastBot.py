import asyncio
import datetime
import json
import math
import random
import sys

from aiocfscrape import CloudflareScraper
import re
import time
import traceback
import fake_useragent
import aiohttp
import async_db
import steam_currencies
from async_db import *
common_start = time.time()

from steampy.client import SteamClient
import pickle
import traceback
import os



from steampy.models import GameOptions

db = async_db.Storage()


class Bot:
    def __init__(self, steam_data: dict = None, proxies: dict = None):
        self.steam_data = steam_data
        if steam_data:
            self.login = steam_data['login']
            self.password = steam_data['password']
            self.shared_secret = steam_data['shared_secret']
            self.identity_secret = steam_data['identity_secret']
            self.steamid = steam_data['steamid']
            self.api_key = steam_data['web_api']
            self.session = None
            self.login_achieved = False

    def steam_login(self, proxy: dict = None) -> SteamClient:

        if not os.path.exists('steampy_sessions'):
            os.mkdir('steampy_sessions')
            print('Создал файл для сессий')

        for i in range(5):
            session_alive = None
            steam_json = self.steam_data
            try:
                print(f'[{self.login}] Вход в аккаунт...')
                if os.path.isfile(f"steampy_sessions/{self.login}.pkl"):
                    print(f'[{self.login}] Подгрузка cookies...')
                    with open(f"steampy_sessions/{self.login}.pkl", "rb") as file:
                        steam_client = pickle.load(file)
                    try:
                        session_alive = steam_client.is_session_alive()
                    except:
                        steam_client = SteamClient(api_key=self.api_key, proxies=proxy)
                        steam_client.login(self.login, self.password, steam_json)
                        with open(f"steampy_sessions/{self.login}.pkl", "wb") as file:
                            pickle.dump(steam_client, file)
                        if steam_client.is_session_alive():
                            break

                    if not session_alive:
                        print(f'[{self.login}] Cookies устарели. Логинимся в аккаунт...')
                        steam_client = SteamClient(api_key=self.api_key, proxies=proxy)
                        steam_client.login(self.login, self.password, steam_json)
                        with open(f"steampy_sessions/{self.login}.pkl", "wb") as file:
                            pickle.dump(steam_client, file)
                        if steam_client.is_session_alive():
                            break
                    else:
                        break
                else:
                    print(f'[{self.login}] Логинимся в аккаунт...')
                    steam_client = SteamClient(api_key=self.api_key, proxies=proxy)
                    steam_client.login(self.login, self.password, steam_json)
                    with open(f"steampy_sessions/{self.login}.pkl", "wb") as file:
                        pickle.dump(steam_client, file)
                    if steam_client.is_session_alive():
                        break
                print(f'[{self.login}] Выполнен вход в аккаунт')
            except:
                pass
                print(traceback.format_exc())
        return steam_client


steam_data = {
    "login": "heatherruiz0m",
    "password": "iRCkt89MGwDJG9xuu2hZR6",
    "shared_secret": "YwN2W7CkpTr5RheOFb2to+5T7z8=",
    "steamid": "76561199174186940",
    "identity_secret": "qqoDnsdzeIX0v/kU1HkJnJM2x4g=",
    "web_api": "4DFD64A220956231A036A808F6FFF376"
}
proxy = {
    'https': 'http://user206276:j97rcb@212.52.5.18:8831',
    'http': 'http://user206276:j97rcb@212.52.5.18:8831,'
}
api_key = 'D3D00BF26E986C73B046D5AECC405129'
username = 'heatherruiz0m'
password = 'iRCkt89MGwDJG9xuu2hZR6'

steam_bot = Bot(steam_data)



class Element:
    def __init__(self, element: str):
        self.element = element
        self.tries = 3

    def __repr__(self):
        return self.element


class FastBot:

    def __init__(self):
        self.response_counter = 0
        self.skin_counter = set()
        self.currencies = {
            "2001": "USD",
            "2002": "GBP",
            "2003": "EURO",
            "2004": "CHF",
            "2005": "RUB",
            "2006": "PLN",
            "2007": "BRL",
            "2008": "JPY",
            "2009": "NOK",
            "2010": "IDR",
            "2011": "MYR",
            "2012": "PHP",
            "2013": "SGD",
            "2014": "THB",
            "2015": "VND",
            "2016": "KRW",
            "2017": "TRY",
            "2018": "UAH",
            "2019": "MXN",
            "2020": "CAD",
            "2021": "AUD",
            "2022": "NZD",
            "2023": "CNY",
            "2024": "INR",
            "2025": "CLP",
            "2026": "PEN",
            "2027": "COP",
            "2028": "ZAR",
            "2029": "HKD",
            "2030": "TWD",
            "2031": "SAR",
            "2032": "AED",
            "2033": "SEK",
            "2034": "ARS",
            "2035": "ILS",
            "2036": "BYN",
            "2037": "KZT",
            "2038": "KWD",
            "2039": "QAR",
            "2040": "CRC",
            "2041": "UYU",
            "2042": "BGN",
            "2043": "HRK",
            "2044": "CZK",
            "2045": "DKK",
            "2046": "HUF",
            "2047": "RON"
        }

        with open('proxies.txt', 'r', encoding='utf-8') as file:
            self.proxies = [
                Element('http://' + line.strip().split(':')[2] + ':' + line.strip().split(':')[3] + '@' +
                        line.strip().split(':')[
                            0] + ':' + line.strip().split(':')[1]) for line in file.readlines()]

        self.responses = {proxy.element: [] for proxy in self.proxies}
        self.ts = []

        self.available_proxies = []
        self.links = []
        self.params = {'count': 100}
        self.headers = {'User-Agent': fake_useragent.UserAgent().chrome}
        self.start_time = time.time()
        self.currency_converter = None
        #self.loop = asyncio.get_event_loop()


    def convert_name_to_link(self, name: str, appid: str = '730'):
        url = name.replace(' ', '%20').replace('#',
                                               '%23').replace(
            ',', '%2C').replace('|', '%7C')
        link = f"https://steamcommunity.com/market/listings/{appid}/{url}"
        return link

    def get_info_from_text(self, item_text: str, appid='730', time_=604800, page_num=0) -> dict:
        """Принимает текст страницы, возвращает все данные о нем на ТП"""

        data = {
            'history': None,
            'skins_info': None
        }

        assets_line = re.search(r'var g_rgAssets = (.+);', item_text)

        try:
            if assets_line and json.loads(assets_line.group(1)):
                assets = json.loads(assets_line.group(1))[appid]['2']
            else:
                return {}

            listing_info_line = re.search(r'var g_rgListingInfo = (.+);', item_text)

            item_nameid_line = re.search(r'Market_LoadOrderSpread\( (.+?) \);', item_text)
            item_nameid = item_nameid_line.group(1)
        except:
            print('\nНет лотов')
            return {}


        history_line = re.search(r'var line1=(.+);', item_text)
        if history_line:
            history = tuple(
                [(datetime.datetime.strptime(i[0], '%b %d %Y %H: +%S').timestamp(), i[1], int(i[2])) for i in
                 json.loads(history_line.group(1)) if
                 datetime.datetime.now().timestamp() - datetime.datetime.strptime(i[0],
                                                                                  '%b %d %Y %H: +%S').timestamp() < time_])
            data['history'] = history


        if listing_info_line:
            listing_info = json.loads(listing_info_line.group(1))
            data['skins_info'] = dict()
            default_price = math.inf
            default_price_without_fee = math.inf
            
            for idx, (listing_id, value) in enumerate(listing_info.items()):
                if idx > min(len(listing_info), 100):
                    break
                currencyid = self.currencies[str(value['currencyid'])]
                amount = int(value['asset']['amount'])
                market_actions = value['asset'].get('market_actions')
                asset_id = value['asset']['id']
                fee = value['fee']
                price = value['price'] + fee
                rub_price = round(self.currency_converter['RUB'] * price / self.currency_converter[currencyid] / 100, 2)
                steam_without_fee = round(
                    self.currency_converter['RUB'] * value['price'] / self.currency_converter[currencyid] / 100, 2)
                if rub_price < default_price:
                    default_price = rub_price
                    default_price_without_fee = steam_without_fee


                if market_actions:
                    link = market_actions.pop()['link'].replace('listingid', listing_id).replace('assetid', asset_id)
                else:
                    link = None
                sticker_value = assets[asset_id]['descriptions'][-1]['value']


                if amount:
                    data['skins_info'][listing_id] = {
                        'listingid': listing_id,
                        'assetid': asset_id,
                        'currencyid': currencyid,
                        'price': rub_price,
                        'default_price': default_price,
                        'default_price_without_fee': default_price_without_fee,
                        'link': link,
                        'stickers': [],
                        'item_nameid': item_nameid,
                        'steam_without_fee': steam_without_fee,
                        'page_num': page_num}

                    if sticker_value != ' ':

                        special_stickers = ['Run T,$$$ Run',
                                            'Run CT,$$$ Run',
                                            "Don't Worry,$$$ I'm Pro",
                                            'Hi,$$$ My Game Is',
                                            'Rock,$$$ Paper,$$$ Scissors']
                        
                        pattern = r"<br>Sticker: (.+?)<"
                        if re.search(pattern, sticker_value):
                            
                            line = list(re.search(pattern, sticker_value).group(1))
                            
                            count_of_brackets = 1
                            for ch in range(len(line)):
                                if line[ch] == '(':
                                    count_of_brackets -= 1
                                elif line[ch] == ')':
                                    count_of_brackets += 1
                                if count_of_brackets and line[ch] == ',':
                                    line[ch] = ',$$$'
                            if line:
                                raw_stickers = ''.join(line)
                                for sp_st in special_stickers:
                                    raw_stickers = raw_stickers.replace(sp_st, sp_st.replace('$$$', ''))
                                stickers = list(map(lambda name: 'Sticker | ' + name, raw_stickers.split(',$$$ ')))
                    
                                data['skins_info'][listing_id]['stickers'] = stickers
        return data

    async def fetch_item(self, session: aiohttp.ClientSession, url_el: Element, proxy: str, idx: int,
                         appid: str = '730', db_connection=None, sticker=False):
        url = self.convert_name_to_link(url_el.element, appid)
        headers = {
            "User-Agent": fake_useragent.UserAgent().random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }

        params = {
            'country': 'PL',
            'appid': appid,
            'market_hash_name': url_el.element,
            'currency': 1,
            'count': 100
        }

        # Независимая задержка для каждого запроса
        t1 = random.randint(8900, 9000) / 1000 * (idx // len(self.proxies))
        await self.delayed_request(session, url, headers, params, proxy, db_connection, t1, url_el, idx, appid)

    async def delayed_request(self, session, url, headers, params, proxy, db_connection, t1, url_el, idx, appid, max_parse=300):

        await asyncio.sleep(t1)  # Задержка перед выполнением запроса



        # Выполнение самого запроса
        info = {
            'code': 429,
            'info': {},
            'url': url
        }
        common_skins_info = dict()
        try:
            print(
                f'\r[{datetime.datetime.now()}] Проверено: {len(self.succeded)} Сделано запросов: {self.response_counter} Со стикерами, но без наценки: {len(self.skin_counter)} RPS: {round(self.response_counter / (time.time() - self.start_time), 2)}',
                end='', flush=True)

            

            for start in range(0, max_parse, 100):
                self.response_counter += 1
                params['start'] = start
                async with session.get(url=url, headers=headers, params=params, proxy=proxy, ssl=True) as response:
                    if response.status == 200:

                        item_text = (await response.text()).strip()

                        info = self.get_info_from_text(item_text=item_text, appid=appid, page_num=int(start/100))

                        self.stop_parsing = False
                    elif response.status == 429:
                        print(f'[{datetime.datetime.now()}] Слишком много запросов. Пробуем еще раз...',  proxy)
                        await asyncio.sleep(300)
                        raise Exception


                    if info:
                        if len(common_skins_info | info['skins_info']) == len(common_skins_info):
                            break
                        else:
                            common_skins_info |= info['skins_info']
                            if 0 < len(info['skins_info']) < 100:
                                break
                            await asyncio.sleep(9)
                    else:
                        break

            info = common_skins_info

            if info:
                default_price = math.inf
                default_price_without_fee = math.inf
                for k, v in info.items():
                    if v['default_price'] < default_price:
                        default_price = v['default_price']
                        default_price_without_fee = v['default_price_without_fee']

                skin_lots = []
                #
                for k, v in info.items():
                    market_actions_link = v['link']
                    buy_id = v['listingid']
                    
                    skin = url_el.element
                    id_ = v['assetid']
                    sticker = v['stickers']
                    #default_price = v['default_price']
                    #default_price_without_fee = v['default_price_without_fee']
                    listing_price = v['price']
                    # (buy_id, skin, id, listing_price, default_price, steam_without_fee, sticker, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    page_num = v['page_num']
                    steam_without_fee = v['steam_without_fee']
                    wear = []
                    sticker_slot = []
                    sticker_price = []
                    sticker_addition_price = 0

                    if sticker:
                        for st in sticker:
                            st_price = 0

                            if st in self.stickers:
                                st_price = self.stickers[st]
                                if sticker.count(st) == 2:
                                    sticker_addition_price += (st_price * 0.2)
                                elif sticker.count(st) == 3:
                                    sticker_addition_price += (st_price * 0.25)
                                elif sticker.count(st) >= 4:
                                    sticker_addition_price += (st_price * 0.3)
                                elif sticker.count(st) == 1:
                                    
                                    if st_price > 30000:
                                        sticker_addition_price += (st_price * 0.045)
                                    elif 10000 < st_price < 30000:
                                        sticker_addition_price += (st_price * 0.085)
                                    elif st_price < 10000:
                                        sticker_addition_price += (st_price * 0.095)
                                
                            else:
                                await asyncio.sleep(3)
                                response_from_sticker = await self.delayed_request(session=session,
                                                                                   url=self.convert_name_to_link(st),
                                                                                   headers=headers,
                                                                                   params=params,
                                                                                   proxy=proxy,
                                                                                   db_connection=db_connection,
                                                                                   t1=7,
                                                                                   url_el=url_el,
                                                                                   idx=1,
                                                                                   appid=appid,
                                                                                   max_parse=100)

                                if st in self.stickers and response_from_sticker['info']:
                                    st_price = self.stickers[st]
                                elif response_from_sticker['info']:
                                    for key in response_from_sticker['info']:
                                        st_price = response_from_sticker['info'][key]['default_price']
                                        break
                                    if st_price:
                                        self.stickers[st] = st_price
                                        print(f'\r[{datetime.datetime.now()}] Выгружаю стикеры в бд...', end='',
                                              flush=True)
                                        dbt1 = time.time()
                                        await db_connection.add_sticker(st, st_price, datetime.datetime.now())
                                        dbt2 = time.time()
                                        self.start_time += (dbt2 - dbt1) * 1.6
                                        print(f'\r[{datetime.datetime.now()}] Стикеры выгружены в бд', end='',
                                              flush=True)

                                st_price = float(st_price)

                                if st_price:
                                    self.stickers[st] = st_price
                                if sticker.count(st) == 2:
                                    sticker_addition_price += (st_price * 0.2)
                                elif sticker.count(st) == 3:
                                    sticker_addition_price += (st_price * 0.25)
                                elif sticker.count(st) >= 4:
                                    sticker_addition_price += (st_price * 0.3)
                                elif sticker.count(st) == 1:
                                    
                                    if st_price > 30000:
                                        sticker_addition_price += (st_price * 0.045)
                                    elif 10000 < st_price < 30000:
                                        sticker_addition_price += (st_price * 0.085)
                                    elif st_price < 10000:
                                        sticker_addition_price += (st_price * 0.095)

                            sticker_price.append([st, st_price])

                        link_to_found = f"{url}?filter={' '.join(sticker).replace('Sticker | ', '')}"
                        wear = [[st, 0] for st in sticker]
                        sticker_slot = [[sticker[i], i] for i in range(len(sticker))]

                    else:
                        link_to_found = url
                    if sticker and sticker_addition_price == 0:
                        self.skin_counter.add(buy_id)
                    profit = round(default_price_without_fee + sticker_addition_price - listing_price, 2)
                    percent = round(profit / listing_price * 100, 2)
                    ts = datetime.datetime.now()
                    # ('ЩАС БУДУ ДОБАВЛЯТЬ', (str(buy_id), str(skin), str(id_), str(listing_price), str(default_price), str(steam_without_fee), str(sticker), str(link_to_found), str(ts), str(wear), str(sticker_slot), str(sticker_price), str(profit), str(percent)))
                    if 'Sticker |' in str(skin) and skin not in self.stickers:
                        print(f'\r[{datetime.datetime.now()}] Выгружаю стикеры в бд...', end='', flush=True)

                        dbt1 = time.time()
                        await db_connection.add_sticker(skin, default_price, ts)
                        dbt2 = time.time()
                        self.start_time += (dbt2 - dbt1) * 1.6
                        print(f'\r[{datetime.datetime.now()}] Стикеры выгружены в бд', end='', flush=True)
                        self.stickers[skin] = default_price
                    elif 'Sticker |' not in str(skin):
                        # ('MARKET ACTIONS', market_actions_link)
                        if (percent > 0 and sticker) or (idx > int(self.len_links * divis) and percent > -10):
                            skin_lots.append((str(buy_id), str(skin), str(id_), str(listing_price), str(default_price),
                                              str(steam_without_fee), str(sticker), str(link_to_found), str(ts),
                                              str(wear), str(sticker_slot), str(sticker_price), str(profit),
                                              str(percent), str(market_actions_link), '1', page_num, str(buy_id)))
                        else:
                            if not (percent > 0):
                                print(f"Условие не выполнено: percent > 0 (percent = {percent})")
                            if not sticker:
                                print(f"Условие не выполнено: sticker (sticker = {sticker})")

                            if not (idx > int(self.len_links * divis)):
                                print(
                                    f"Условие не выполнено: idx > int(self.len_links * divis) (idx = {idx}, divis = {divis})")
                            if not (percent > -10):
                                print(f"Условие не выполнено: percent > -10 (percent = {percent})")

                #('SKIN LOTSSSS', len(skin_lots), skin_lots)
                if skin_lots:
                    print(f'\r[{datetime.datetime.now()}] Выгружаю скины в бд...', end='', flush=True)

                    dbt1 = time.time()
                    await db_connection.smthmany(skin_lots)
                    dbt2 = time.time()

                    self.start_time += (dbt2 - dbt1) * 1.6
                    print(f'\r[{datetime.datetime.now()}] Скины выгружены в бд', end='', flush=True)
                if url_el in self.links:
                    self.links.remove(url_el)
                self.succeded.append(url_el)

            info = {
                'code': response.status,
                'info': info,
                'url': url
            }
        except:
            print(traceback.format_exc())
        finally:
            return info


    async def parse_items(self, list_of_names: list, appid: str):
        self.currency_converter = await steam_currencies.main()
        if self.currency_converter:
            if self.currency_converter['RUB'] < 1:
                self.currency_converter = await steam_currencies.main()
        


        self.stop_parsing = False
        await self.check_proxy()

        self.succeded, self.failed = [], []
        self.links = [Element(name) for name in list_of_names]
        self.len_links = int(len(self.links))

        async with async_db.Storage() as db:

            self.start_time = time.time()
            self.response_counter = 0
            self.stickers = await db.fetch_all_stickers()

            if not self.links:
                return

            # Создаем список задач, распределяя ссылки по потокам
            tasks = []
            async with aiohttp.ClientSession() as session:
                session.cookie_jar.update_cookies(cookies)
                for idx, link in enumerate(self.links):
                    proxy = self.proxies[idx % len(self.proxies)].element
                    tasks.append(asyncio.create_task(self.fetch_item(session, link, proxy, idx, db_connection=db)))
                # Параллельное выполнение всех запросов
                await asyncio.gather(*tasks)

    async def check_proxy(self):
        proxy_list = self.proxies
        self.available_proxies = [proxy for proxy in
                                  await asyncio.gather(*[self.ping_proxy(proxy) for proxy in proxy_list]) if proxy]

    async def ping_proxy(self, proxy):
        async with aiohttp.ClientSession() as session:

            async with session.get(url='https://steamcommunity.com/', proxy=proxy.element, ssl=False) as response:

                if response.status == 200:
                    return proxy
                else:
                    return None


async def analyze_data():
    async with async_db.Storage() as db:
        await db.analyze_all_base()

async def insert_data():
    async with async_db.Storage() as db:
        await db.insert_data()

while True:
    asyncio.run(analyze_data())

    start_console_ts = time.time()

    try:
        bot = FastBot()
        with open('baza730.txt', 'r', encoding='utf-8') as file:
            baza730 = file.read().split('\n')
        with open('floats.txt', 'r', encoding='utf-8') as file:
            floats = file.read().split('\n')

        floats *= 2

        names = list(set(baza730 + floats))

        client = steam_bot.steam_login()
        cookies = client._session.cookies
        step = len(bot.proxies) * 5

        divis = 0.75

        while baza730:

            if time.time() - start_console_ts > 900:
                os.system('cls')
                start_console_ts = time.time()

            pack_of_names = []
            for i in range(int(step*divis)):
                if baza730:
                    pack_of_names.append(baza730.pop(0))

            for i in range(int(step - divis * divis)):
                if floats:
                    pack_of_names.append(floats.pop(0))

            start = time.time()
            asyncio.run(bot.parse_items(list_of_names=pack_of_names, appid='730'))
            print('Цикл завершился за', time.time() - start)
            time.sleep(10)
    except:
        print(traceback.format_exc())
    finally:
        time.sleep(10)
