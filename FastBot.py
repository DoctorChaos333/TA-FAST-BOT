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
from dateutil import parser
from decimal import Decimal
from steampy.client import SteamClient
import pickle
import traceback
import os



from steampy.models import GameOptions

# db = async_db.Storage()


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

    async def steam_login(self, proxy: dict = None) -> SteamClient:
        async with async_db.Storage() as db:
            await db.add_log_entry(self.login, proxy, "Запуск steam_login")

        if not os.path.exists('steampy_sessions'):
            os.mkdir('steampy_sessions')
            async with async_db.Storage() as db:
                await db.add_log_entry(self.login, proxy, "Создана директория для сессий")

        for attempt in range(5):
            session_alive = None
            steam_json = self.steam_data
            try:
                async with async_db.Storage() as db:
                    await db.add_log_entry(self.login, proxy, f"Попытка входа №{attempt + 1}")

                if os.path.isfile(f"steampy_sessions/{self.login}.pkl"):
                    async with async_db.Storage() as db:
                        await db.add_log_entry(self.login, proxy, "Файл cookies найден, загрузка cookies")
                    with open(f"steampy_sessions/{self.login}.pkl", "rb") as file:
                        steam_client = pickle.load(file)
                    try:
                        session_alive = steam_client.is_session_alive()
                        async with async_db.Storage() as db:
                            await db.add_log_entry(self.login, proxy, "Сессия активна")
                    except Exception as e:
                        async with async_db.Storage() as db:
                            await db.add_log_entry(self.login, proxy, f"Ошибка проверки сессии: {str(e)}")
                        steam_client = SteamClient(api_key=self.api_key, proxies=proxy)
                        steam_client.login(self.login, self.password, steam_json)
                        with open(f"steampy_sessions/{self.login}.pkl", "wb") as file:
                            pickle.dump(steam_client, file)
                        if steam_client.is_session_alive():
                            async with async_db.Storage() as db:
                                await db.add_log_entry(self.login, proxy, "Сессия восстановлена после ошибки")
                            break

                    if not session_alive:
                        async with async_db.Storage() as db:
                            await db.add_log_entry(self.login, proxy, "Сессия устарела, требуется повторный вход")
                        steam_client = SteamClient(api_key=self.api_key, proxies=proxy)
                        steam_client.login(self.login, self.password, steam_json)
                        with open(f"steampy_sessions/{self.login}.pkl", "wb") as file:
                            pickle.dump(steam_client, file)
                        if steam_client.is_session_alive():
                            async with async_db.Storage() as db:
                                await db.add_log_entry(self.login, proxy, "Сессия успешно восстановлена")
                            break
                    else:
                        async with async_db.Storage() as db:
                            await db.add_log_entry(self.login, proxy, "Сессия активна")
                        break
                else:
                    async with async_db.Storage() as db:
                        await db.add_log_entry(self.login, proxy, "Файл cookies не найден, выполняем вход")
                    steam_client = SteamClient(api_key=self.api_key, proxies=proxy)
                    steam_client.login(self.login, self.password, steam_json)
                    with open(f"steampy_sessions/{self.login}.pkl", "wb") as file:
                        pickle.dump(steam_client, file)
                    if steam_client.is_session_alive():
                        async with async_db.Storage() as db:
                            await db.add_log_entry(self.login, proxy, "Вход выполнен, сессия активна")
                        break

                async with async_db.Storage() as db:
                    await db.add_log_entry(self.login, proxy, "Вход в аккаунт выполнен успешно")
            except Exception as e:
                async with async_db.Storage() as db:
                    await db.add_log_entry(self.login, proxy, f"Ошибка входа в аккаунт: {traceback.format_exc()}")

        async with async_db.Storage() as db:
            await db.add_log_entry(self.login, proxy, "Завершение попыток входа")
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
            "2001": "USD", "2002": "GBP", "2003": "EURO", "2004": "CHF", "2005": "RUB",
            "2006": "PLN", "2007": "BRL", "2008": "JPY", "2009": "NOK", "2010": "IDR",
            "2011": "MYR", "2012": "PHP", "2013": "SGD", "2014": "THB", "2015": "VND",
            "2016": "KRW", "2017": "TRY", "2018": "UAH", "2019": "MXN", "2020": "CAD",
            "2021": "AUD", "2022": "NZD", "2023": "CNY", "2024": "INR", "2025": "CLP",
            "2026": "PEN", "2027": "COP", "2028": "ZAR", "2029": "HKD", "2030": "TWD",
            "2031": "SAR", "2032": "AED", "2033": "SEK", "2034": "ARS", "2035": "ILS",
            "2036": "BYN", "2037": "KZT", "2038": "KWD", "2039": "QAR", "2040": "CRC",
            "2041": "UYU", "2042": "BGN", "2043": "HRK", "2044": "CZK", "2045": "DKK",
            "2046": "HUF", "2047": "RON"
        }

        # Загрузка прокси
        with open('proxies.txt', 'r', encoding='utf-8') as file:
            self.proxies = [
                Element('http://' + line.strip().split(':')[2] + ':' + line.strip().split(':')[3] + '@' +
                        line.strip().split(':')[0] + ':' + line.strip().split(':')[1]) for line in file.readlines()]

        self.responses = {proxy.element: [] for proxy in self.proxies}
        self.ts = []
        self.available_proxies = []
        self.links = []
        self.params = {'count': 100}
        self.headers = {'User-Agent': fake_useragent.UserAgent().chrome}
        self.start_time = time.time()
        self.currency_converter = None

    async def async_init(self):
        """Асинхронный метод для инициализации логов и других асинхронных задач."""
        await self.log("FastBot", None, "Инициализация FastBot начата")
        await self.log("FastBot", None, f"Загружено {len(self.proxies)} прокси")
        await self.log("FastBot", None, "Инициализация FastBot завершена")

    async def log(self, item, proxy_used, message, thread_id=None):
        """Асинхронная функция для записи лога."""
        async with async_db.Storage() as db:
            await db.add_log_entry(item, proxy_used, message, thread_id)

    async def convert_name_to_link(self, name: str, appid: str = '730'):
        url = name.replace(' ', '%20').replace('#', '%23').replace(',', '%2C').replace('|', '%7C')
        link = f"https://steamcommunity.com/market/listings/{appid}/{url}"

        # Используем await для асинхронного вызова log
        await self.log("convert_name_to_link", None, f"Ссылка на {name}: {link}")

        return link

    async def get_info_from_text(self, item_text: str, appid='730', time_=604800, page_num=0, thread_id=None) -> dict:
        """Принимает текст страницы, возвращает все данные о нем на ТП"""

        data = {
            'history': None,
            'skins_info': None
        }

        # Предполагается, что proxy доступен как атрибут self
        proxy = getattr(self, 'proxy', None)

        # Начало обработки
        await self.log("delayed_request", proxy, "Начало обработки текста страницы.")

        assets_line = re.search(r'var g_rgAssets = (.+);', item_text)

        try:
            if assets_line:
                assets_json = json.loads(assets_line.group(1))
                # Логируем, чтобы увидеть текущую структуру данных
                await self.log("delayed_request", proxy,
                               f"Содержимое assets_json (первые 500 символов): {json.dumps(assets_json)[:500]}", thread_id)

                if isinstance(assets_json, dict) and appid in assets_json and '2' in assets_json[appid]:
                    assets = assets_json[appid]['2']
                    await self.log("delayed_request", proxy, f"Assets успешно извлечены: {len(assets)} активов.", thread_id)
                else:
                    await self.log("delayed_request", proxy,
                                   "Assets не найдены или структура не соответствует ожиданиям.", thread_id)
                    return {}
            else:
                await self.log("delayed_request", proxy, "Assets не найдены.", thread_id)
                return {}

            listing_info_line = re.search(r'var g_rgListingInfo = (.+);', item_text)
            item_nameid_line = re.search(r'Market_LoadOrderSpread\( (.+?) \);', item_text)

            if not listing_info_line or not item_nameid_line:
                await self.log("delayed_request", proxy, "Не удалось найти listing_info_line или item_nameid_line.", thread_id)
                return {}

            listing_info = json.loads(listing_info_line.group(1))
            item_nameid = item_nameid_line.group(1)
            await self.log("delayed_request", proxy, "Listing info и item_nameid успешно извлечены.", thread_id)

        except Exception as e:
            error_message = f"Ошибка при извлечении assets или listing_info: {e}\n{traceback.format_exc()}"
            await self.log("delayed_request", proxy, error_message, thread_id)
            return {}

        history_line = re.search(r'var line1=(.+);', item_text)
        if history_line:
            try:
                history = tuple(
                    [
                        (
                            datetime.datetime.strptime(i[0], '%b %d %Y %H: +%S').timestamp(),
                            i[1],
                            int(i[2])
                        ) for i in json.loads(history_line.group(1))
                        if datetime.datetime.now().timestamp() - datetime.datetime.strptime(i[0],
                                                                                            '%b %d %Y %H: +%S').timestamp() < time_
                    ]
                )
                data['history'] = history
                await self.log("delayed_request", proxy, f"История успешно обработана: {len(history)} записей.", thread_id)
            except Exception as e:
                await self.log("delayed_request", proxy, f"Ошибка при обработке истории: {e}", thread_id)

        if listing_info_line:
            try:
                data['skins_info'] = {}
                default_price = math.inf
                default_price_without_fee = math.inf

                for idx, (listing_id, value) in enumerate(listing_info.items()):
                    if idx > min(len(listing_info), 100):
                        await self.log("delayed_request", proxy, "Достигнут лимит в 100 лотов.", thread_id)
                        break

                    currencyid = self.currencies[str(value['currencyid'])]
                    amount = int(value['asset']['amount'])
                    market_actions = value['asset'].get('market_actions')
                    asset_id = value['asset']['id']
                    fee = value['fee']
                    price = value['price'] + fee
                    rub_price = round(
                        self.currency_converter['RUB'] * price / self.currency_converter[currencyid] / 100, 2)
                    steam_without_fee = round(
                        self.currency_converter['RUB'] * value['price'] / self.currency_converter[currencyid] / 100, 2
                    )
                    if rub_price < default_price:
                        default_price = rub_price
                        default_price_without_fee = steam_without_fee

                    if market_actions:
                        link = market_actions.pop()['link'].replace('listingid', listing_id).replace('assetid',
                                                                                                     asset_id)
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
                            'page_num': page_num
                        }

                        if sticker_value != ' ':

                            special_stickers = [
                                'Run T,$$$ Run',
                                'Run CT,$$$ Run',
                                "Don't Worry,$$$ I'm Pro",
                                'Hi,$$$ My Game Is',
                                'Rock,$$$ Paper,$$$ Scissors'
                            ]

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

                await self.log("delayed_request", proxy,
                               f"skins_info успешно обработаны: {len(data['skins_info'])} записей.", thread_id)
            except Exception as e:
                await self.log("delayed_request", proxy, f"Ошибка при обработке skins_info: {e}", thread_id)

        await self.log(
            "delayed_request",
            proxy,
            f"Запрос успешен, данные извлечены: {len(data.get('skins_info', {}))} записей.", thread_id
        )

        return data

    async def fetch_item(self, session: aiohttp.ClientSession, url_el: Element, proxy: str, idx: int,
                         appid: str = '730', db_connection=None, sticker=False):
        thread_id = url_el.element
        url = await self.convert_name_to_link(url_el.element, appid)
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

        # Лог начала выполнения fetch_item
        await self.log("fetch_item", proxy, f"Запуск fetch_item для {url_el.element}, idx: {idx}", thread_id)

        # Независимая задержка для каждого запроса
        t1 = random.randint(8900, 9000) / 1000 * (idx // len(self.proxies))
        await self.delayed_request(session, url, headers, params, proxy, db_connection, t1, url_el, idx, appid)

    async def delayed_request(self, session, url, headers, params, proxy, db_connection, t1, url_el, idx, appid,
                              max_parse=300):
        thread_id = url_el.element
        await asyncio.sleep(t1)  # Задержка перед выполнением запроса
        await self.log("delayed_request", proxy, f"Задержка перед запросом {t1} секунд для {url}", thread_id)

        info = {'code': 429, 'info': {}, 'url': url}
        common_skins_info = {}

        try:
            await self.log("delayed_request", proxy, f"Выполнение запроса {url}", thread_id)
            for start in range(0, max_parse, 100):
                self.response_counter += 1
                params['start'] = start
                async with session.get(url=url, headers=headers, params=params, proxy=proxy, ssl=True) as response:
                    if response.status == 200:
                        item_text = (await response.text()).strip()
                        info = await self.get_info_from_text(item_text=item_text, appid=appid,
                                                             page_num=int(start / 100), thread_id=thread_id)
                        self.stop_parsing = False
                        await self.log("delayed_request", proxy,
                                       f"Запрос успешен, данные извлечены: {len(info)} записей", thread_id)
                    elif response.status == 429:
                        await self.log("delayed_request", proxy, "Слишком много запросов, повтор через 300 секунд", thread_id)
                        await asyncio.sleep(300)
                        raise Exception("Превышено количество запросов")

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
            await self.log("delayed_request", proxy, f"Общий сбор данных завершен, всего записей: {len(info)}", thread_id)
            skin = None
            if info:
                passed_items = {
                    'cheap_stickers': 0,
                    'float, percent < percent' : 0,
                    'not sticker': 0
                }
                default_price = math.inf
                default_price_without_fee = math.inf
                skin_lots = []

                added_to_temp = 0
                have_stickers = 0


                for k, v in info.items():
                    if v['default_price'] < default_price:
                        default_price = v['default_price']
                        default_price_without_fee = v['default_price_without_fee']

                    market_actions_link = v['link']
                    buy_id = v['listingid']
                    skin = url_el.element
                    id_ = v['assetid']
                    sticker = v['stickers']
                    listing_price = v['price']
                    page_num = v['page_num']
                    steam_without_fee = v['steam_without_fee']
                    wear = []
                    sticker_slot = []
                    sticker_price = []
                    sticker_addition_price = 0

                    if sticker:
                        for st in sticker:
                            st_price = self.stickers.get(st, 0)
                            if st in self.stickers:
                                # Добавляем наценку для различных случаев количества стикеров
                                if sticker.count(st) == 2:
                                    sticker_addition_price += st_price * 0.2
                                elif sticker.count(st) == 3:
                                    sticker_addition_price += st_price * 0.25
                                elif sticker.count(st) >= 4:
                                    sticker_addition_price += st_price * 0.3
                                elif sticker.count(st) == 1:
                                    if st_price > 30000:
                                        sticker_addition_price += st_price * 0.045
                                    elif 10000 < st_price <= 30000:
                                        sticker_addition_price += st_price * 0.085
                                    else:
                                        sticker_addition_price += st_price * 0.095
                            else:
                                await asyncio.sleep(3)
                                response_from_sticker = await self.delayed_request(
                                    session=session,
                                    url=await self.convert_name_to_link(st),
                                    headers=headers,
                                    params=params,
                                    proxy=proxy,
                                    db_connection=db_connection,
                                    t1=7,
                                    url_el=url_el,
                                    idx=1,
                                    appid=appid,
                                    max_parse=100
                                )
                                await self.get_sticker_price(session=session, sticker_name=st, proxy=proxy)

                                if st in self.stickers and response_from_sticker['info']:
                                    st_price = self.stickers[st]
                                elif response_from_sticker['info']:
                                    for key in response_from_sticker['info']:
                                        st_price = response_from_sticker['info'][key]['default_price']
                                        break
                                    if st_price:
                                        self.stickers[st] = st_price
                                        dbt1 = time.time()
                                        async with async_db.Storage() as db:
                                            await db.add_sticker(st, st_price, datetime.datetime.now())
                                        await self.log("delayed_request", proxy, f"Обновил цену на стикер {st}: {st_price}",
                                                       thread_id)
                                        dbt2 = time.time()
                                        self.start_time += (dbt2 - dbt1) * 1.6

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
                                else:
                                    await self.log("delayed_request", proxy, f"Не нашел цену на {st}",
                                                   thread_id)

                            # Добавляем стикер и его цену в соответствующие списки
                            sticker_price.append([st, st_price])
                            wear.append([st, 0])  # Пример значений для wear, нужно настроить по специфике данных
                            sticker_slot.append([sticker[i], i] for i in range(len(sticker)))

                    # Формируем окончательные значения и вычисления для базы данных
                    link_to_found = f"{url}?filter={' '.join(sticker).replace('Sticker | ', '')}" if sticker else url
                    profit = round(default_price_without_fee + sticker_addition_price - listing_price, 2)
                    percent = round(profit / listing_price * 100, 2)
                    ts = datetime.datetime.now()
                    sticker_slot = [list([sticker[i], i]) for i in range(len(sticker))]

                    if 'Sticker |' in str(skin) and skin not in self.stickers:


                        dbt1 = time.time()
                        async with async_db.Storage() as db:
                            await db.add_sticker(skin, default_price, ts)
                        await self.log("delayed_request", proxy, f"Добавил стикер {skin} : {default_price} в бд",
                                       thread_id)
                        dbt2 = time.time()
                        self.start_time += (dbt2 - dbt1) * 1.6
                        self.stickers[skin] = default_price
                    elif 'Sticker |' not in str(skin):

                        if (percent > 0 and sticker) or (idx > int(self.len_links * divis) and percent > -30):
                            if sticker:
                                have_stickers += 1
                            added_to_temp += 1
                            skin_lots.append((str(buy_id), str(skin), str(id_), str(listing_price), str(default_price),
                                              str(steam_without_fee), str(sticker), str(link_to_found), str(ts),
                                              str(wear), str(sticker_slot), str(sticker_price), str(profit),
                                              str(percent), str(market_actions_link), '1', page_num))
                        elif percent < 0 and sticker:
                            passed_items['cheap_stickers'] += 1
                        elif idx > int(self.len_links * divis) and percent > -30:
                            passed_items['float, percent < percent'] += 1
                        elif not sticker:
                            passed_items['not sticker'] += 1
                await self.log("delayed_request", proxy, f"Было: {len(info)}. Отсеял: {str(passed_items)}. Добавлю: {len(skin_lots)}",
                               thread_id)


                if skin_lots:
                    if skin is not None:
                        async with async_db.Storage() as db:
                            await db.dump_statistics(skin=skin, added_to_temp=added_to_temp, have_stickers=have_stickers)

                    await self.log("delayed_request", proxy,
                                   f"Скины подготовлены для базы, всего: {len(skin_lots)} записей", thread_id)
                    await self.log("delayed_request", proxy, f"Что грузим: {[i[2] for i in skin_lots]}", thread_id)
                    async with async_db.Storage() as db:
                        await db.smthmany(skin_lots)
                    await self.log("delayed_request", proxy, f"Скины выгружены в базу, всего: {len(skin_lots)} записей", thread_id)

            info = {'code': response.status, 'info': info, 'url': url}

        except Exception as e:
            await self.log("delayed_request", proxy, f"Ошибка при выполнении запроса: {traceback.format_exc()}", thread_id)
        finally:
            return info

    async def parse_items(self, list_of_names: list, appid: str, cookies):
        async with async_db.Storage() as db:
            await db.add_log_entry("parse_items_start", None, f"Начало parse_items для {len(list_of_names)} имен")

        # Логирование конвертации валют
        self.currency_converter = await steam_currencies.main()
        async with async_db.Storage() as db:
            await db.add_log_entry("currency_conversion", None,
                                   f"Конвертер валют установлен: {self.currency_converter}")
        if self.currency_converter:
            if self.currency_converter['RUB'] < 1:
                self.currency_converter = await steam_currencies.main()
                async with async_db.Storage() as db:
                    await db.add_log_entry("currency_converter_update", None,
                                           "Обновлена конвертация валют из-за некорректного курса RUB")

        # Логирование проверки прокси
        self.stop_parsing = False
        #await self.check_proxy()
        self.available_proxies = [proxy for proxy in self.proxies]
        async with async_db.Storage() as db:
            await db.add_log_entry("proxy_check", None,
                                   f"Проверка прокси завершена, доступных прокси: {len(self.available_proxies)}")

        # Логирование подготовленного списка ссылок
        self.succeded, self.failed = [], []
        self.links = [Element(name) for name in list_of_names]
        self.len_links = int(len(self.links))
        async with async_db.Storage() as db:
            await db.add_log_entry("link_preparation", None, f"Подготовлено {self.len_links} ссылок для обработки")

        # Логирование работы с базой
        async with async_db.Storage() as db:
            await db.add_log_entry("parse_items_init_storage", None,
                                   f"Инициализация Storage для обработки {len(self.links)} ссылок")

        # Логирование начала работы
        self.start_time = time.time()
        self.response_counter = 0
        async with async_db.Storage() as db:
            self.stickers = await db.fetch_all_stickers()
        async with async_db.Storage() as db:
            await db.add_log_entry("sticker_fetch", None, f"Получены стикеры: {len(self.stickers)}")

        if not self.links:
            async with async_db.Storage() as db:
                await db.add_log_entry("parse_items_no_links", None, "Нет ссылок для обработки, завершение parse_items")
            return

        # Логирование асинхронного выполнения запросов
        tasks = []
        async with aiohttp.ClientSession() as session:
            session.cookie_jar.update_cookies(cookies)
            async with async_db.Storage() as db:
                await db.add_log_entry("session_cookies", None, "Обновлены cookies для сессии")

            for idx, link in enumerate(self.links):
                proxy = self.proxies[idx % len(self.proxies)].element
                tasks.append(asyncio.create_task(self.fetch_item(session, link, proxy, idx)))
                async with async_db.Storage() as db:
                    await db.add_log_entry("fetch_task_creation", proxy,
                                           f"Создана задача fetch_item для {link.element}")

            await asyncio.gather(*tasks)
            async with async_db.Storage() as db:
                await db.add_log_entry("fetch_tasks_complete", None, "Все задачи fetch_item завершены")

        async with async_db.Storage() as db:
            await db.add_log_entry("parse_items_end", None, "parse_items завершена успешно")

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

    async def get_sticker_price(self, session, sticker_name, proxy):
        await asyncio.sleep(7)
        url = await self.convert_name_to_link(sticker_name)
        headers = {
            "User-Agent": fake_useragent.UserAgent().random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        params = {
            'country': 'PL',
            'appid': 730,
            'market_hash_name': sticker_name,
            'currency': 5
        }

        try:
            async with session.get(url=url, headers=headers, params=params, proxy=proxy, ssl=True) as response:
                if response.status == 200:
                    item_text = (await response.text()).strip()
                    history_line = re.search(r'var line1=(.+);', item_text)
                    if history_line:
                        try:
                            data = json.loads(history_line.group(1))
                            current_date = datetime.datetime.now()
                            cutoff_date = current_date - datetime.timedelta(days=15)

                            sorted_data = sorted([
                                entry[1] for entry in data
                                if datetime.datetime.strptime(entry[0], "%b %d %Y %H: +0") >= cutoff_date for _ in
                                range(int(entry[2]))
                            ])
                            n = len(sorted_data)

                            # Если количество элементов нечётное
                            if n % 2 == 1:
                                median = sorted_data[n // 2]
                            else:
                                # Если количество элементов чётное
                                mid1 = n // 2 - 1
                                mid2 = n // 2
                                median = (sorted_data[mid1] + sorted_data[mid2]) / 2
                            if median:
                                #self.stickers[sticker_name] = median
                                async with async_db.Storage() as db:
                                    await db.add_sticker(sticker_name, median, datetime.datetime.now())
                                await self.log("get_sticker_price", proxy, f"{sticker_name}: {median} RUB", -1)
                                #return median
                        except Exception as e:
                            await self.log("get_sticker_price", proxy, f"Ошибка при обработке истории: {e}", -1)

        except Exception as e:
            await self.log("get_sticker_price", proxy, f"Ошибка при обработке истории: {e}", -1)



async def analyze_data():
    async with async_db.Storage() as db:
        await db.analyze_all_base()

async def insert_data():
    async with async_db.Storage() as db:
        await db.insert_data()


async def main():
    while True:
        # async with async_db.Storage() as db:
        #     await db.add_log_entry("analyze_data", None, "Запуск analyze_data()")
        # await analyze_data()

        start_console_ts = time.time()
        async with async_db.Storage() as db:
            await db.add_log_entry("main_loop_start", None,
                                   f"Начало основного цикла, время запуска: {start_console_ts}")

        try:
            async with async_db.Storage() as db:
                await db.add_log_entry("init", None, "Инициализация FastBot и SteamBot")

            bot = FastBot()
            await bot.async_init()
            with open('baza730.txt', 'r', encoding='utf-8') as file:
                baza730 = file.read().split('\n')
            async with async_db.Storage() as db:
                await db.add_log_entry("file_load", "baza730.txt", f"Файл baza730 загружен, элементов: {len(baza730)}")

            with open('floats.txt', 'r', encoding='utf-8') as file:
                floats = file.read().split('\n')
            async with async_db.Storage() as db:
                await db.add_log_entry("file_load", "floats.txt", f"Файл floats загружен, элементов: {len(floats)}")

            floats *= 2

            names = list(set(baza730))

            async with async_db.Storage() as db:
                await db.add_log_entry("data_combination", None,
                                       f"Сформирован список names, уникальных элементов: {len(names)}")

            client = await steam_bot.steam_login()
            async with async_db.Storage() as db:
                await db.add_log_entry("steam_login", None, "Вход в Steam выполнен")

            cookies = client._session.cookies
            async with async_db.Storage() as db:
                await db.add_log_entry("session_cookies", None, "Cookies установлены для сессии")

            step = len(bot.proxies) * 5

            global divis
            divis = 0.75

            while baza730:
                if time.time() - start_console_ts > 900:
                    os.system('cls')
                    start_console_ts = time.time()
                    async with async_db.Storage() as db:
                        await db.add_log_entry("console_clear", None, "Консоль очищена из-за таймаута 900 секунд")

                pack_of_names = []
                for i in range(int(step * divis)):
                    if baza730:
                        pack_of_names.append(baza730.pop(0))
                async with async_db.Storage() as db:
                    await db.add_log_entry("pack_creation", None,
                                           f"Создан пакет из baza730, размер: {len(pack_of_names)}")

                for i in range(int(step - divis * divis)):
                    if floats:
                        pack_of_names.append(floats.pop(0))
                async with async_db.Storage() as db:
                    await db.add_log_entry("pack_creation", None,
                                           f"Добавлено из floats, общий размер пакета: {len(pack_of_names)}")

                start = time.time()
                async with async_db.Storage() as db:
                    await db.add_log_entry("parse_items_start", None,
                                           f"Запуск parse_items для {len(pack_of_names)} имен")
                await bot.parse_items(list_of_names=pack_of_names, appid='730', cookies=cookies)
                end = time.time()

                async with async_db.Storage() as db:
                    await db.add_log_entry("parse_items_end", None,
                                           f"parse_items завершена, время выполнения: {end - start} секунд")

                await asyncio.sleep(10)
        except Exception as e:
            async with async_db.Storage() as db:
                await db.add_log_entry("main_loop_exception", None,
                                       f"Ошибка в основном цикле: {traceback.format_exc()}")
        finally:
            async with async_db.Storage() as db:
                await db.add_log_entry("main_loop_sleep", None, "Пауза 10 секунд перед перезапуском цикла")
            await asyncio.sleep(10)


# Запускаем бесконечный цикл
asyncio.run(main())

