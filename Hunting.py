import math
import re
import threading
import time

import aiohttp
import async_db
import fake_useragent
import steam_currencies
from async_db import *

common_start = time.time()

from steampy.client import SteamClient
import pickle
import traceback


class Bot:
    def __init__(self, acc_info: dict = None, proxy: dict = dict()):
        self.steam_data = acc_info
        if acc_info:
            self.login = acc_info['login'].strip()
            self.password = acc_info['password'].strip()
            self.shared_secret = acc_info['shared'].strip()
            self.identity_secret = acc_info['identity'].strip()
            self.steamid = acc_info['steamID'].strip()
            self.api_key = acc_info['API'].strip()
            if acc_info['session']:
                self.session = pickle.loads(acc_info['session'])
            else:
                self.session = None
            self.last_session_ts = acc_info['last_session_ts']
            self.login_achieved = False
            self.proxies = {
                'http': proxy,
                'https': proxy
                }
    def steam_login(self) -> SteamClient:

        for i in range(5):
            session_alive = None
            steam_json = {
                "login": self.login,
                "password": self.password,
                "shared_secret": self.shared_secret,
                "steamid": self.steamid,
                "identity_secret": self.identity_secret,
                "web_api": self.api_key
            }
            try:
                print(f'[{self.login}] Вход в аккаунт...')
                if self.session and self.last_session_ts + datetime.timedelta(hours = 1) > datetime.datetime.now():
                    try:
                        print('Cookies подошли')
                        session_alive = self.session.is_session_alive()
                    except:
                        self.session = SteamClient(api_key=self.api_key, proxies=self.proxies)
                        self.session.login(self.login, self.password, steam_json)
                        asyncio.run(update_session(self.login, pickle.dumps(self.session), datetime.datetime.now()))
                        if self.session.is_session_alive():
                            print('Cookies подошли')
                            break

                    if not session_alive:
                        print(f'[{self.login}] Cookies устарели. Логинимся в аккаунт...')
                        self.session = SteamClient(api_key=self.api_key, proxies=self.proxies)

                        self.session.login(self.login, self.password, steam_json)
                        if self.session.is_session_alive():
                            asyncio.run(update_session(self.login, pickle.dumps(self.session), datetime.datetime.now()))
                            break
                    else:
                        break
                else:
                    print(f'[{self.login}] Логинимся в аккаунт...')
                    self.session = SteamClient(api_key=self.api_key, proxies=self.proxies)
                    self.session.login(self.login, self.password, steam_json)
                    if self.session.is_session_alive():
                        print(f'[{self.login}] Выполнен вход в аккаунт')
                        asyncio.run(update_session(self.login, pickle.dumps(self.session), datetime.datetime.now()))
                        break

            except:
                pass
                print(traceback.format_exc())
        return self.session

async def update_session(login: str, session: bytes, ts):
    async with async_db.Storage() as db:
        return await db.update_session(login, session, ts)



def convert_name_to_link(name: str, appid: str = '730'):
    url = name.replace(' ', '%20').replace('#',
                                           '%23').replace(
        ',', '%2C').replace('|', '%7C')
    link = f"https://steamcommunity.com/market/listings/{appid}/{url}"
    return link


async def ping_proxy(proxy_):
    async with aiohttp.ClientSession() as session:
        async with session.get(url='https://steamcommunity.com/', proxy=proxy_.element, ssl=False) as response:
            if response.status == 200:
                return proxy_
            else:
                return None


class FastBot:

    def __init__(self, cookies):
        self.stop_parsing = None
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

        #with open('proxies.txt', 'r', encoding='utf-8') as file:
        #    self.proxies = [
        #        Element('http://' + line.strip().split(':')[2] + ':' + line.strip().split(':')[3] + '@' +
        #                line.strip().split(':')[
        #                    0] + ':' + line.strip().split(':')[1]) for line in file.readlines()]

        #self.responses = {proxy.element: [] for proxy in self.proxies}
        self.ts = []
        self.cookies = cookies
        self.available_proxies = []
        self.links = []
        self.params = {'count': 100}
        self.headers = {'User-Agent': fake_useragent.UserAgent().chrome}
        self.start_time = time.time()
        #self.currency_converter = None

    def get_info_from_text(self, item_text: str, appid='730', time_=604800, page_num = 0) -> dict:
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
                fee = value['converted_fee']
                price = value['converted_price'] + fee
                #rub_price = round(self.currency_converter['RUB'] * price / self.currency_converter[currencyid] / 100, 2)
                steam_without_fee = value['converted_price']
                if price < default_price:
                    default_price = price
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
                        'price': price,
                        'default_price': default_price,
                        'default_price_without_fee': default_price_without_fee,
                        'fee': fee,
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

    async def fetch_item(self, session: aiohttp.ClientSession, info, proxy: str, idx: int,
                         appid: str = '730', db_connection=None, account_id=None):

        session.cookie_jar.update_cookies(self.cookies[info['login']]['cookie'])
        print(cookies[info['login']]['login'], cookies[info['login']]['currency'])
        skin = info['skin']
        max_percent = info['max_percent_buy']


        url = convert_name_to_link(skin, appid)
        headers = {
            "User-Agent": fake_useragent.UserAgent().random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }

        params = {
            'country': 'PL',
            'appid': appid,
            'market_hash_name': info['skin'],
            'count': 100
        }

        # Независимая задержка для каждого запроса
        t1 = random.randint(10200, 10400) / 1000 * (idx // len(self.hunting_info)) + 0.01

        await self.delayed_request(session=session, url=url, headers=headers, params=params, proxy=proxy, db=db_connection, t1=t1, skin=skin, appid=appid, account_id=account_id, max_percent=max_percent, login=info['login'])

    async def delayed_request(self, session, url, headers, params, proxy, db, t1, skin, appid, account_id, max_percent,
                              max_parse=300, login=''):

        await asyncio.sleep(t1)  # Задержка перед выполнением запроса
        # if (self.response_counter / len(self.proxies)) % 15 == 0 and self.response_counter > 0:
        #    print(
        #        f'\r[{datetime.datetime.now()}] Перерыв в запросах...',
        #        end='', flush=True)
        #    await asyncio.sleep(75)

        # Выполнение самого запроса
        info = {
            'code': 429,
            'info': {},
            'url': url
        }
        common_skins_info = dict()
        try:
            print(
                f'\r[{datetime.datetime.now()}] Сделано запросов: {self.response_counter} Со стикерами, но без наценки: {len(self.skin_counter)} RPS: {round(self.response_counter / (time.time() - self.start_time), 2)}',
                end='', flush=True)

            for start in range(0, max_parse, 100):
                self.response_counter += 1
                params['start'] = start
                session.cookie_jar.update_cookies(self.cookies[login]['cookie'])
                async with session.get(url=url, headers=headers, params=params, proxy=proxy, ssl=True) as response:
                    if response.status == 200:

                        # if (self.response_counter % 10) == 0:
                        #    print(f"{self.response_counter} RPS: {round(self.response_counter / (time.time() - self.start_time), 2)}")
                        item_text = (await response.text()).strip()
                        print('SKIN', cookies[login]['currency'], skin)
                        if """"wallet_currency":1""" in item_text:
                            print(f"wallet_currency = 1")
                        elif """"wallet_currency":5""" in item_text:
                            print(f"wallet_currency = 5")
                        elif """"wallet_currency":37""" in item_text:
                            print(f"wallet_currency = 37")
                        print(self.cookies[login]['client'].get_wallet_balance())
                        info = self.get_info_from_text(item_text=item_text, appid=appid, page_num=int(start/100))

                        self.stop_parsing = False
                    elif response.status == 429:
                        print(f'[{datetime.datetime.now()}] Слишком много запросов. Пробуем еще раз...', proxy)
                        await asyncio.sleep(300)
                        raise Exception
                    else:
                        print('RESPONSE STATUS', response.status)

                    if info:
                        if len(common_skins_info | info['skins_info']) == len(common_skins_info):
                            break
                        else:
                            common_skins_info |= info['skins_info']
                            if 0 < len(info['skins_info']) < 100:
                                break
                            await asyncio.sleep(8)
                    else:
                        print('NOT INFO')
                        break

            info = common_skins_info
            needed_to_parse = []
            if info:
                default_price = math.inf
                default_price_without_fee = math.inf
                for k, v in info.items():
                    if v['default_price'] < default_price:
                        default_price = v['default_price']
                        default_price_without_fee = v['default_price_without_fee']


                for k, v in info.items():
                    market_actions_link = v['link']
                    buy_id = v['listingid']
                    #print('BUY_ID', buy_id)
                    listing_price = v['price']
                    fee = v['fee']
                    sticker = v['stickers']
                    page_num = v['page_num']
                    wear = [[st, 0] for st in sticker]
                    percent = int(100 * listing_price / default_price_without_fee - 100)
                    ts = datetime.datetime.now()

                    skin = skin
                    id_ = v['assetid']
                    if id_ not in self.floats and percent < max_percent:
                        needed_to_parse.append((str(id_), '1', market_actions_link, listing_price, default_price, str(sticker), str(wear), account_id, buy_id, ts, page_num, fee))
                    listing_price = v['price']

            info = {
                'code': response.status,
                'info': info,
                'url': url
            }
            if needed_to_parse:
                print(f'\r[{datetime.datetime.now()}] Выгружаю скины в базу...')
                await db.add_to_spam_hunting_temp(tuple(needed_to_parse))
        except:

            print(3)

            print(traceback.format_exc())
        finally:
            return info

    async def start_hunting(self):
        #self.currency_converter = await steam_currencies.main()

        #await self.check_proxy()

        #self.links = [Element(name) for name in list_of_names]


        async with async_db.Storage() as db:
            self.floats = await db.get_all_floats()
            self.proxies = await db.fetch_hunting_proxies(ip=4)

            print(f'ITERATOR {iterator}')

            self.hunting_info = await db.get_hunting_info()
            self.hunting_info = random.sample(self.hunting_info, min([len(self.proxies), len(self.hunting_info)]))
            #print(self.hunting_info)


            self.skins = [item['skin'] for item in self.hunting_info]
            self.start_time = time.time()

            # Создаем список задач, распределяя ссылки по потокам
            tasks = []

            async with aiohttp.ClientSession() as session:
                for idx, info in enumerate(self.hunting_info * 10):
                    proxy = self.proxies[idx % len(self.proxies)]
                    tasks.append(asyncio.create_task(self.fetch_item(session, info, proxy, idx, db_connection=db, account_id=info['id'])))
                # Параллельное выполнение всех запросов

                await asyncio.gather(*tasks)

    async def check_proxy(self):
        proxy_list = self.proxies
        self.available_proxies = [pr for pr in
                                  await asyncio.gather(*[ping_proxy(pr) for pr in proxy_list]) if pr]



#client = steam_bot.steam_login()
#cookies = client._session.cookies

async def get_proxies_and_hunting_info():
    async with async_db.Storage() as db:
        proxies = await db.fetch_hunting_proxies()
        hunting_info = await db.get_hunting_info()
        users = [await db.get_spam_user(acc['login']) for acc in hunting_info]
        return [proxies, users]

cookies = dict()

def get_cookie(account, proxy):
    acc: SteamClient = Bot(acc_info = account, proxy=proxy)
    session= acc.steam_login()
    cookies[account['login']] = {'cookie': session._session.cookies,
                                'client': session,
                                'currency': account['currency'],
                                 'login': account['login']}



while True:
    for iterator in range(100):
        if iterator % 20 == 0:
            proxies, hunting_info = asyncio.run(get_proxies_and_hunting_info())
            threads = [threading.Thread(target=get_cookie, args=value) for value in
                       zip(hunting_info, proxies)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        bot = FastBot(cookies)
        start = time.time()
        asyncio.run(bot.start_hunting())
        time.sleep(4)
