import aiohttp
import async_db
import asyncio
import time
import datetime

from steampy.client import SteamClient
import pickle
import traceback
from steampy.models import GameOptions, Currency
import re
import json
import fake_useragent


currencies = {
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

def get_info_from_text(item_text: str) -> dict:
    """Принимает текст страницы, возвращает все данные о нем на ТП"""

    data = dict()

    try:
        listing_info_line = re.search(r'var g_rgListingInfo = (.+);', item_text)
    except:
        print('\nНет лотов')
        return {}

    if listing_info_line:
        listing_info = json.loads(listing_info_line.group(1))
        if listing_info:
            for idx, (listing_id, value) in enumerate(listing_info.items()):
                if idx > min(len(listing_info), 100):
                    break
                amount = int(value['asset']['amount'])
                converted_fee = value.get('converted_fee')
                converted_price = value.get('converted_price')


                if amount:
                    data[listing_id] = {
                        'listingid': listing_id,
                        'price': converted_price,
                        'fee': converted_fee}
    return data

class Bot:
    def __init__(self, acc_info: dict = None):
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
                        self.session = SteamClient(api_key=self.api_key, proxies=proxy)
                        self.session.login(self.login, self.password, steam_json)
                        asyncio.run(update_session(self.login, self.session))
                        if self.session.is_session_alive():
                            print('Cookies подошли')
                            break

                    if not session_alive:
                        print(f'[{self.login}] Cookies устарели. Логинимся в аккаунт...')
                        self.session = SteamClient(api_key=self.api_key, proxies=proxy)

                        self.session.login(self.login, self.password, steam_json)
                        if self.session.is_session_alive():
                            asyncio.run(update_session(self.login, pickle.dumps(self.session), datetime.datetime.now()))
                            break
                    else:
                        break
                else:
                    print(f'[{self.login}] Логинимся в аккаунт...')
                    self.session = SteamClient(api_key=self.api_key, proxies=proxy)
                    self.session.login(self.login, self.password, steam_json)
                    if self.session.is_session_alive():
                        print(f'[{self.login}] Выполнен вход в аккаунт')
                        asyncio.run(update_session(self.login, pickle.dumps(self.session), datetime.datetime.now()))
                        break

            except:
                pass
                print(traceback.format_exc())
        return self.session



proxy = {
    'https': 'http://TATGoWcragRwuETA:m0sgyj@195.96.159.131:1310',
    'http': 'http://TATGoWcragRwuETA:m0sgyj@195.96.159.131:1310,'
}

async def ticket_table_checker():
    async with async_db.Storage() as db:
        return await db.check_new_tickets()

async def get_spam_users():
    async with async_db.Storage() as db:
        return await db.get_spam_users()

async def get_spam_user(login: str):
    async with async_db.Storage() as db:
        return await db.get_spam_user(login=login)

async def update_session(login: str, session: bytes, ts):
    async with async_db.Storage() as db:
        return await db.update_session(login, session, ts)

async def update_ticket_status(buy_id: str, status: int):
    async with async_db.Storage() as db:
        return await db.update_ticket_status(buy_id, status)


def main():
    while True:
        time.sleep(0.5)
        new_tickets = asyncio.run(ticket_table_checker())
        if new_tickets:

            autobuy_spam_users = [u for u in asyncio.run(get_spam_users()) if u['auto_buy']]
            #Список из словарей вида {'login': 'rVtRvcJbjY', 'password': '2uDzYalSPTB', 'steamID': '76561199513749651', 'shared': '3bhL51ug5dMycDbFcTlSxEOdWnw=', 'identity': 'KvZIBt1wwfPyQkBnbMJ1RVE5LH8=', 'session': None, 'API': '4510CAB674C19960D68CA6FFA50A08BB', 'last_session_ts': datetime.datetime(2024, 9, 23, 23, 23, 44), 'currency': 'KZT', 'auto_buy': 1, 'auto_buy_percent': 80, 'tariff': 299}


            for ticket in new_tickets:
                print(f'\r[{datetime.datetime.now()}] Нашел новый тикет {ticket}', end='', flush=True)

                try:
                    login = ticket['steam_login']
                    user = asyncio.run(get_spam_user(login))

                    if True:
                        bot = Bot(user)
                        session = bot.steam_login()
                        currency = user['currency']
                        skin = ticket['skin']
                        buy_id = ticket['buy_id']
                        currency = eval(f"Currency.{currency}")
                        is_hunting=ticket['is_hunting']
                        price = ticket['price']
                        fee = ticket['fee']
                        asyncio.run(update_ticket_status(buy_id, -1))
                        buy_ticket_item(session, skin, buy_id, appid='730', currency=currency, is_hunting=is_hunting, price=price, fee=fee)

                except:
                    asyncio.run(update_ticket_status(buy_id, 0))
        else:
            print(f'\r[{datetime.datetime.now()}] Нет новых тикетов', end='', flush=True)

def convert_name_to_link(name: str, appid: str = '730'):
    url = name.replace(' ', '%20').replace('#',
                                           '%23').replace(
        ',', '%2C').replace('|', '%7C')
    link = f"https://steamcommunity.com/market/listings/{appid}/{url}"
    return link

def buy_ticket_item(session: SteamClient, skin, buy_id, appid='730', currency=Currency.RUB, page_num=-1, is_hunting=0, price=None, fee=None):
    url = convert_name_to_link(skin, appid)
    params = {'count': 100}
    headers = {'User-Agent': fake_useragent.UserAgent().chrome}
    
    for _ in range(3):
        if is_hunting == 1:
            try:
                response = session.market.buy_item(market_name=skin,
                                                   market_id=buy_id,
                                                   price=price, fee=fee,
                                                   game=GameOptions.CS,
                                                   currency=currency)
                if response['wallet_info']['success']:
                    asyncio.run(update_ticket_status(buy_id, 1))
                    print('Купил предмет', buy_id)
                    return
                else:
                    asyncio.run(update_ticket_status(buy_id, 0))
            except:
                print('Пока не удалось купить предмет', buy_id)
                time.sleep(3)
                continue
    else:
        asyncio.run(update_ticket_status(buy_id, 0))
    for _ in range(3):
        with session._session.get(url=url, headers=headers, params=params) as response:
            if response.status_code == 200:
                item_text = response.text
                info = get_info_from_text(item_text)
                if info:
                    if buy_id in info:
                        price = info[buy_id]['price']
                        fee = info[buy_id]['fee']
                        for i in skin, buy_id, price, fee, currency:
                            print(i, type(i))
                        time.sleep(3)
                        try:
                            response = session.market.buy_item(market_name=skin,
                                                    market_id=buy_id,
                                                    price=price+fee, fee=fee,
                                                    game=GameOptions.CS,
                                                    currency=currency)
                            if response['wallet_info']['success']:
                                asyncio.run(update_ticket_status(buy_id, 1))
                                print('Купил предмет', buy_id)
                                return
                            else:
                                asyncio.run(update_ticket_status(buy_id, 0))
                        except:
                            print('Пока не удалось купить предмет', buy_id)
                            time.sleep(3)
                            continue
                    else:
                        print('buy id not in info')
                        print(info)
                else:
                    print('NOT INFO')
            else:
                print(response.status_code)
    else:
        asyncio.run(update_ticket_status(buy_id, 0))

if __name__ == '__main__':
    main()
