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
        self.loop = asyncio.get_event_loop()


    def convert_name_to_link(self, name: str, appid: str = '730'):
        url = name.replace(' ', '%20').replace('#',
                                               '%23').replace(
            ',', '%2C').replace('|', '%7C')
        link = f"https://steamcommunity.com/market/listings/{appid}/{url}"
        return link

    def get_info_from_text(self, item_text: str, appid='730', time_=604800) -> dict:
        """Принимает текст страницы, возвращает все данные о нем на ТП"""

        data = {
            'history': None,
            'skins_info': None
        }

        assets_line = re.search(r'var g_rgAssets = (.+);', item_text)
        try:
            if assets_line and json.loads(assets_line.group(1)):
                assets = json.loads(assets_line.group(1))[appid]['2']
                # print('ОШШИБКА', json.loads(assets_line.group(1))[appid])
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
                        'steam_without_fee': steam_without_fee}

                    if sticker_value != ' ':
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
                                stickers = list(map(lambda name: 'Sticker | ' + name, ''.join(line).split(',$$$ ')))
                                data['skins_info'][listing_id]['stickers'] = stickers
        return data

    async def fetch_item(self, session: aiohttp.ClientSession, url_el: Element, proxy: str, idx: int,
                         appid: str = '730', db_connection=None):
        url = self.convert_name_to_link(url_el.element, appid)
        proxy = proxy

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

        t1 = idx * random.randint(3700, 3800) / 1000 + (idx // 15) * 70
        await asyncio.sleep(t1)

        info = {
            'code': 429,
            'info': {},
            'url': url
        }

        if self.stop_parsing:
            await asyncio.sleep(120)

        try:
            self.responses[proxy].append(round(time.time() - self.start_time, 1))
            print(
                f'\r[{datetime.datetime.now()}] Проверено: {len(self.succeded)} RPS: {round(self.response_counter / (time.time() - self.start_time), 2)}',
                end='', flush=True)
            async with session.get(url=url, headers=headers, params=params, proxy=proxy, timeout=15, ssl=False) as response:
                info = {}
                if response.status == 200:
                    self.response_counter += 1
                    #if (self.response_counter % 10) == 0:
                    #    print(f"{self.response_counter} RPS: {round(self.response_counter / (time.time() - self.start_time), 2)}")
                    item_text = (await response.text()).strip()
                    info = self.get_info_from_text(item_text=item_text, appid=appid)
                    self.stop_parsing = False
                elif response.status == 429:
                    print(f'[{datetime.datetime.now()}] Слишком много запросов. Пробуем еще раз...')
                    raise Exception
                else:
                    print(response.status)
                if info:
                    skin_lots = []
                    #
                    for k, v in info['skins_info'].items():
                        market_actions_link = v['link']
                        buy_id = v['listingid']
                        skin = url_el.element
                        id_=v['assetid']
                        sticker=v['stickers']
                        default_price = v['default_price']
                        default_price_without_fee = v['default_price_without_fee']
                        listing_price = v['price']
                        #(buy_id, skin, id, listing_price, default_price, steam_without_fee, sticker, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
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
                                else:
                                    #print('В базе нет', st)
                                    await asyncio.sleep(5)
                                    if (idx // 15) * 70 > 0:
                                        print('Буду ожидать дополнительно')
                                    response_from_sticker = await self.fetch_item(session, Element(st), proxy, idx, appid, db_connection)

                                    if st in self.stickers and response_from_sticker['info']:
                                        st_price = self.stickers[st]

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
                        profit = round(default_price_without_fee + sticker_addition_price - listing_price, 2)
                        percent = round(profit / listing_price * 100, 2)
                        ts = datetime.datetime.now()
                        #print('ЩАС БУДУ ДОБАВЛЯТЬ', (str(buy_id), str(skin), str(id_), str(listing_price), str(default_price), str(steam_without_fee), str(sticker), str(link_to_found), str(ts), str(wear), str(sticker_slot), str(sticker_price), str(profit), str(percent)))
                        if 'Sticker |' in str(skin) and skin not in self.stickers:
                            print(f'\r[{datetime.datetime.now()}] Выгружаю стикеры в бд...', end='', flush=True)
                            await db_connection.add_sticker(skin, default_price, ts)
                            print(f'\r[{datetime.datetime.now()}] Стикеры выгружены в бд', end='', flush=True)
                            self.stickers[skin] = default_price
                        elif 'Sticker |' not in str(skin):
                            #print('MARKET ACTIONS', market_actions_link)
                            skin_lots.append((str(buy_id), str(skin), str(id_), str(listing_price), str(default_price),
                                              str(steam_without_fee), str(sticker), str(link_to_found), str(ts),
                                              str(wear), str(sticker_slot), str(sticker_price), str(profit),
                                              str(percent), str(market_actions_link), '1'))
                    #print('SKIN LOTSSSS', len(skin_lots), skin_lots)
                    if skin_lots:
                        print(f'\r[{datetime.datetime.now()}] Выгружаю скины в бд...', end='', flush=True)
                        await db_connection.smthmany(skin_lots)
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
            print(3)
            print(traceback.format_exc())
        finally:
            return info

    async def parse_float(self, market_actions_link: str, session, proxy):
        url = f"https://floats.steaminventoryhelper.com/?url={market_actions_link.replace('%20M%', '%20M').replace('%A%', 'A').replace('%D', 'D')}"
        #scraper = cloudscraper.create_scraper(browser={'browser': 'firefox','platform': 'windows','mobile': False})
        headers = {
            'authority': 'floats.steaminventoryhelper.com',
            'method': 'GET',
            'scheme': 'https',
            'Accept': '*/*',
            'Accept-encoding': 'gzip, deflate, br, zstd',
            'Accept-language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
            'Cache-control': 'no-cache',
            'Pragma': 'no-cache',
            'Priority': 'u=1, i',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'none',
            'User-Agent': fake_useragent.UserAgent().random,
            'X-Sih-Version': '2.0.19'
        }
        async with CloudflareScraper() as session:
            async with session.get(url=url, headers=headers, ssl=False) as response:
                response_json = await response.text()
                print('Такой вот response прилетает', response.status, response_json)


    async def parse_items(self, list_of_names: list, appid: str):
        self.currency_converter = await steam_currencies.main()

        self.stop_parsing = False

        await self.check_proxy()

        self.succeded, self.failed = [], []

        self.links = [Element(name) for name in list_of_names]

        while True:
            try:
                async with async_db.Storage() as db:
                    self.start_time = time.time()
                    self.response_counter = 0
                    self.stickers = await db.fetch_all_stickers()

                    if not self.links:
                        break

                    link_threads = [[] for _ in range(min(len(self.proxies), len(self.links)))]

                    for i in range(len(self.links)):
                        link_threads[i % len(link_threads)].append(self.links[i])

                    async with aiohttp.ClientSession() as session:
                        session.cookie_jar.update_cookies(cookies)
                        tasks = [self.fetch_item(session, link_threads[j][i], self.proxies[j].element, i, db_connection=db) for i in
                                 range(len(link_threads[-1])) for j in range(min(len(self.proxies), len(link_threads)))]

                        await asyncio.gather(*tasks)
                    for el in self.links:
                        el.tries -= 1
                        if el.tries < 1:
                            self.links.remove(el)

            except:
                print(traceback.format_exc())
                print('Ожидаю несколько секунд')
                for s in range(300):
                    print(f'\rОсталось ждать: {300 - s}', end='', flush=True)
                    time.sleep(1)

    async def check_proxy(self):
        proxy_list = self.proxies
        self.available_proxies = [proxy for proxy in
                                  await asyncio.gather(*[self.ping_proxy(proxy) for proxy in proxy_list]) if proxy]

    async def ping_proxy(self, proxy):
        async with aiohttp.ClientSession() as session:
            # print(f'Проверяю прокси {proxy.element}')
            async with session.get(url='https://steamcommunity.com/', proxy=proxy.element, ssl=False) as response:
                # print(f'Проверил прокси {proxy.element}')
                if response.status == 200:
                    return proxy
                else:
                    return None


bot = FastBot()

with open('baza730.txt', 'r', encoding='utf-8') as file:
    baza730 = file.read().split('\n')
with open('floats.txt', 'r', encoding='utf-8') as file:
    floats = file.read().split('\n')
names = list(set(baza730 + floats))
names = ['AWP | Electric Hive (Minimal Wear)']

while True:
    client = steam_bot.steam_login()
    cookies = client._session.cookies
    step = len(bot.proxies) * 5
    for i in range(0, len(names), step):
        i1 = i
        i2 = i + step
        if i2 > len(names):
            i2 = len(names)
        start = time.time()
        asyncio.run(bot.parse_items(list_of_names=names[i1:i2], appid='730'))