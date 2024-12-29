import aiohttp
import async_db
import asyncio
import time
import datetime
from random import random
from steampy.client import SteamClient
import pickle
import traceback
from steampy.models import GameOptions, Currency
import re
import json
import fake_useragent
import ast


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
    with open('item_text_ex.txt', 'w', encoding='utf-8') as file:
        print(item_text, file=file)

    try:
        listing_info_line = re.search(r'var g_rgListingInfo = (.+);', item_text)
    except:
        print('\nНет лотов')
        return {}





    if listing_info_line:
        wallet_currency = re.search(r'var g_rgWalletInfo = (.+);', item_text)
        if wallet_currency:
            data['wallet_currency'] = currencies[str(eval(wallet_currency.group(1))['wallet_currency'] + 2000)]

        else:
            data['wallet_currency'] = None
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
                        'fee': converted_fee
                    }
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
    'https': 'http://TATGoWcragRwuETA:m0sgyj@172.111.130.72:2429',
    'http': 'http://TATGoWcragRwuETA:m0sgyj@172.111.130.72:2429,'
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

async def get_last_items(last_time: datetime.datetime) -> dict:
    async with async_db.Storage() as db:
        return await db.last_items(last_time)

async def update_currency(login: str, currency: str) -> None:
    async with async_db.Storage() as db:
        await db.update_currency(login, currency)

async def create_ticket(steam_login: str, skin: str, buy_id: str, page_num: int, sticker: int) -> None:
    async with async_db.Storage() as db:
        await db.create_ticket(steam_login, skin, buy_id, page_num, sticker)


async def log(item, proxy_used, message, thread_id=None):
    """Асинхронная функция для записи лога."""
    async with async_db.Storage() as db:
        await db.add_log_entry(item, proxy_used, message, thread_id, table_name='log_entries_AB')

async def get_usd():
    async with async_db.Storage() as db:
        rate = await db.get_usd()
        return rate



def convert_name_to_link(name: str, appid: str = '730'):
    url = name.replace(' ', '%20').replace('#',
                                           '%23').replace(
        ',', '%2C').replace('|', '%7C')
    link = f"https://steamcommunity.com/market/listings/{appid}/{url}"
    return link

def buy_ticket_item(session: SteamClient, skin, buy_id, appid='730', currency=Currency.RUB, page_num=0, is_hunting=0, price=None, fee=None, login=None, has_sticker=0):
    url = convert_name_to_link(skin, appid)
    params = {
        'count': 100,
        'start': page_num * 100
              }
    if has_sticker:
        params['filter'] = 'Sticker'
    headers = {'User-Agent': fake_useragent.UserAgent().chrome}

    for _ in range(3):
        if is_hunting == 1:
            try:
                response = session.market.buy_item(
                                                   market_name=skin,
                                                   market_id=buy_id,
                                                   price=price, fee=fee,
                                                   game=GameOptions.CS,
                                                   currency=currency
                )
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

                        if currency is None and info['wallet_currency']:
                            currency = eval(f"Currency.{info['wallet_currency']}")
                            asyncio.run(update_currency(login, info['wallet_currency']))


                        try:
                            if not currency:
                                print('Нет валюты')
                                raise Exception
                            response = session.market.buy_item(
                                                    market_name=skin,
                                                    market_id=buy_id,
                                                    price=price+fee, fee=fee,
                                                    game=GameOptions.CS,
                                                    currency=currency
                            )
                            print('RESPONSE', response, skin, buy_id, price+fee, fee, GameOptions.CS, currency)
                            if response['wallet_info']['success']:
                                asyncio.run(update_ticket_status(buy_id, 1))
                                print('Купил предмет', buy_id)
                                return
                            else:
                                asyncio.run(update_ticket_status(buy_id, 0))
                                print('RESPONSE', response, skin, buy_id, price + fee, fee, GameOptions.CS, currency)
                        except:
                            print('Пока не удалось купить предмет', buy_id)
                            time.sleep(3)
                            continue
                    else:
                        print('buy id not in info')
                else:
                    print('NOT INFO')
            else:
                print(response.status_code)
    else:
        asyncio.run(update_ticket_status(buy_id, 0))



def sort_users(auto_buy_spam_users: list, item: dict):
    usd = 107.815
    suitable_users = []
    item_listing_price = item['listing_price']
    wear = item['wear']
    sticker_slot = item['sticker_slot']
    sticker_price = item['sticker_price']
    current_buy_id = item['buy_id']

    try:
        wear = ast.literal_eval(item['wear'])
        sticker_slot = ast.literal_eval(item['sticker_slot'])
        sticker_price = ast.literal_eval(item['sticker_price'])
    except (ValueError, SyntaxError):
        print(f'[ПРОПУСК] Предмет {current_buy_id}: ошибка при парсинге данных.')
        print("-" * 50)




    for user in auto_buy_spam_users:
        user_min_price = user['min_price']
        user_max_price = user['max_price']
        user_auto_buy_percent = user['auto_buy_percent']



        sticker_summ = 0

        for w, (slot, price) in zip(wear, zip(sticker_slot, sticker_price)):
            if not slot[0]:
                continue
            price_usd = price[1] / usd
            num_occurrences = sum(1 for x in wear if x[0] == w[0])
            sticker_line = f'{slot[0]} ({round(w[1] * 100, 2)}%)*'

            if num_occurrences > 1 and user["use_streak"] == 0 and w[1] == 0:
                streak = True
                sticker_summ_percent = user.get(f"streak_{num_occurrences}", 0)
            else:
                streak = False
                if price_usd > 300:
                    sticker_summ_percent = user.get("more_300", 0)
                elif 100 < price_usd <= 300:
                    sticker_summ_percent = user.get("between100_300", 0)
                else:
                    sticker_summ_percent = user.get("less_100", 0)

            if w[1] == 0 or streak:
                added_price = round(price_usd * sticker_summ_percent / 100, 2)
                sticker_line += f'{round(price_usd, 2)}$*+{added_price}$'
                sticker_summ += added_price
            else:
                sticker_line += f'{round(price_usd, 2)}$*+0$'

        default_price_usd = float(item['default_price']) / usd
        end_price = (default_price_usd + sticker_summ) * 0.87
        listing_price_usd = float(item['listing_price']) / usd
        end_percent = ((end_price * usd) / float(item['listing_price']) - 1) * 100


        if user_min_price <= listing_price_usd <= user_max_price and end_percent >= user_auto_buy_percent:
            suitable_users.append(user)
        if user['login'] == 'elosovetahripunova':
            print(f"[{user['login']} {item['skin']}] !{user_min_price} <= {listing_price_usd} <= {user_max_price}, {end_percent}  ({item['percent']}) >= {user_auto_buy_percent}")


        asyncio.run(log(item=f'{current_buy_id}__', proxy_used=-1, message=f"end_price: {end_price}, end_percent: {end_percent}, chat_id: {user['chat_id']}", thread_id='-1'))

    return suitable_users



def main():
    last_datetime = datetime.datetime.now()
    while True:
        new_items = [item for item in asyncio.run(get_last_items(last_datetime)) if item['buy_id']]
        time.sleep(1)

        if new_items:
            print(f'\n[{datetime.datetime.now()}] Новые предметы:')
            print(*[[item['skin'], item['buy_id']] for item in  new_items], sep='\n')
            autobuy_spam_users = [u for u in asyncio.run(get_spam_users()) if u['auto_buy']]
            for item in new_items:

                suitable_users = sort_users(autobuy_spam_users, item)

                if not suitable_users:
                    continue

                suitable_users.sort(key=lambda x: (x["tariff"], x["auto_buy_percent"]), reverse=True)

                item['login'] = None

                for user in suitable_users:
                    if random() <= 0.5:
                        item['login'] = user['login']
                        asyncio.run(log(item=f"Распределение {item['buy_id']}", proxy_used=-1,
                                        message=f"{item['skin']} {item['buy_id']} закрепляется за {user['login']}"))

                        asyncio.run(create_ticket(user['login'], item['skin'], item['buy_id'], item['page_num'], 1))



                if item['login'] is None:
                    asyncio.run(log(item=f"Распределение {item['buy_id']}", proxy_used=-1,
                                    message=f"{item['skin']} {item['buy_id']} ни за кем не закрепляется"))




            #input('Это были новые предметы')
            #with open('tickets_ex.json', 'w', encoding='utf-8') as file:
            #    json.dump(new_items, fp=file, indent=4)

            #print(f'[{datetime.datetime.now()}] Все пользователи:')
            #print(*autobuy_spam_users, sep='\n')
            #with open('auto_buy_spam_users.json', 'w', encoding='utf-8') as file:
            #    for user in autobuy_spam_users:
            #        user['session'] = 1 if user['session'] else 0
            #        user['last_session_ts'] = 0
            #    json.dump(autobuy_spam_users, fp=file, indent=4)
            last_datetime = datetime.datetime.now()
            #input('Записал примеры данных')
            #Список из словарей вида {'login': 'rVtRvcJbjY', 'password': '2uDzYalSPTB', 'steamID': '76561199513749651', 'shared': '3bhL51ug5dMycDbFcTlSxEOdWnw=', 'identity': 'KvZIBt1wwfPyQkBnbMJ1RVE5LH8=', 'session': None, 'API': '4510CAB674C19960D68CA6FFA50A08BB', 'last_session_ts': datetime.datetime(2024, 9, 23, 23, 23, 44), 'currency': 'KZT', 'auto_buy': 1, 'auto_buy_percent': 80, 'tariff': 299}
        else:
            print(f'\r[{datetime.datetime.now()}] Нет новых предметов', end='', flush=True)
        new_tickets = asyncio.run(ticket_table_checker())
        if new_tickets:


            for ticket in new_tickets:
                print(f'\r[{datetime.datetime.now()}] Нашел новый тикет {ticket}', end='', flush=True)
                buy_id = ticket['buy_id']
                try:
                    login = ticket['steam_login']
                    user = asyncio.run(get_spam_user(login))
                    bot = Bot(user)
                    session = bot.steam_login()
                    currency = user['currency']
                    skin = ticket['skin']
                    page_num = ticket['page_num']
                    has_sticker = ticket['sticker']

                    if currency:
                        currency = eval(f"Currency.{currency}")
                    else:
                        currency = None
                    is_hunting = ticket['is_hunting']
                    price = ticket['price']
                    fee = ticket['fee']

                    if True:

                        asyncio.run(update_ticket_status(buy_id, -1))
                        buy_ticket_item(session, skin, buy_id, appid='730', currency=currency, is_hunting=is_hunting, price=price, fee=fee, login=login, page_num=page_num, has_sticker=has_sticker)

                except:
                    print(traceback.format_exc())

                    asyncio.run(update_ticket_status(buy_id, 0))
        else:
            print(f'\r[{datetime.datetime.now()}] Нет новых тикетов', end='', flush=True)


if __name__ == '__main__':
    main()
