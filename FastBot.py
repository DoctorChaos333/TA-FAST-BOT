import asyncio
import datetime
import itertools
import json
import random
import re
import string
import time
import fake_useragent
import aiohttp

common_start = time.time()

class Element:
    def __init__(self, element: str):
        self.element = element
        self.tries = 10

class FastBot:
    def __init__(self):
        self.currencies = {
            "2001": "USD",
            "2002": "GBP",
            "2003": "EUR",
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


        self.available_proxies = []
        self.links = []

    def convert_name_to_link(self, name: str, appid: str = '730'):
        url = name.replace(' ', '%20').replace('#',
                                               '%23').replace(
            ',', '%2C').replace('|', '%7C')
        link = f"https://steamcommunity.com/market/listings/{appid}/{url}"
        return link

    def get_info_from_text(self, item_text: str, currency: int = 1, appid='730', time_=604800) -> dict:
        """Принимает текст страницы, возвращает все данные о нем на ТП"""

        data = {
            'history': None,
            'skins_info': None
        }

        assets_line = re.search(r'var g_rgAssets = (.+);', item_text)
        try:
            if assets_line and json.loads(assets_line.group(1)):
                assets = json.loads(assets_line.group(1))[appid]['2']
                #print('ОШШИБКА', json.loads(assets_line.group(1))[appid])
            else:
                return {}

            listing_info_line = re.search(r'var g_rgListingInfo = (.+);', item_text)

            item_nameid_line = re.search(r'Market_LoadOrderSpread\( (.+?) \);', item_text)
            item_nameid = item_nameid_line.group(1)
        except:
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

            for idx, (listing_id, value) in enumerate(listing_info.items()):
                if idx > min(len(listing_info), 100):
                    break
                currencyid = value['currencyid']
                amount = int(value['asset']['amount'])
                market_actions = value['asset'].get('market_actions')
                asset_id = value['asset']['id']
                #print(value)
                #print(market_actions)
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
                        'link': link,
                        'stickers': [],
                        'item_nameid': item_nameid}

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

    async def fetch_item(self, link: Element, appid: str, proxy: Element):

        user_agent = fake_useragent.UserAgent().chrome
        item_textag = False
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "origin": "https://steamcommunity.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
            "Sec-Fetch-Site": 'none',
            "Sec-Fetch-Mode": 'cors',
            "Sec-Fetch-Dest": 'empty',
            "sec-ch-ua-platform": '"Windows"',
            "sec-ch-ua-mobile": '?0',
            "sec-ch-ua": '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
            "referer": link.element,
            "Pragma": "no-cache",
            "Host": "steamcommunity.com",
            "DNT": "1",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,de;q=0.6,zh-CN;q=0.5,zh;q=0.4",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "*/*"
        }
        params = {'count': 100}
        #return {
        #    'code': 0,
        #    'link': link,
        #    'proxy': proxy
        #}
        async with aiohttp.ClientSession() as session:
            start = time.time()
            #print(f'Сделал запрос для {link.element}')
            async with session.get(link.element, proxy=proxy.element, headers=headers, params=params, verify_ssl=False) as response:
                time_delta = time.time() - start
                print(f'Сделал запрос через {time.time() - start} для {link.element}')
                if time_delta < 3.5:
                    await asyncio.sleep(start + 3.5 - time.time())
                if response.status == 200:
                    item_text = (await response.text()).strip()
                    info = self.get_info_from_text(item_text=item_text, appid=appid)
                    if info:
                        info = 1
                    else:
                        response.status = 0
                    await session.close()
                    return {
                        'code': response.status,
                        'link': link,
                        'proxy': proxy,
                        'info': info
                    }

                else:
                    print(response.status)
        await session.close()
    async def parse_items(self, list_of_names: list, appid: str):
        await self.check_proxy()
        self.links = [Element(self.convert_name_to_link(link, appid)) for link in list_of_names]

        while self.links:
            last_time = time.time()
            count = min([len(self.proxies), len(self.links)])
            responses = await asyncio.gather(*[self.fetch_item(self.links[i], appid, self.proxies[i]) for i in range(count)])
            current_time = time.time()
            if current_time < last_time + 3.5:
                time.sleep(last_time + 3.5 - current_time)
            #print(*responses, sep='\n')

            counter = 0


            for i in responses:
                if i['code'] != 200:
                    i['link'].tries -= 1
                    i['proxy'].tries -= 1

                    self.proxies.remove(i['proxy'])
                    self.proxies.append(i['proxy'])

                    if i['link'].tries == 0:
                        self.links.remove(i['link'])
                    if i['proxy'].tries == 0:
                        self.proxies.remove(i['proxy'])
                elif i['code'] == 200:
                    counter += 1
                    self.links.remove(i['link'])

            print(f'Спарсилось {counter} скинов за {time.time() - last_time} секунд')
            #for i in self.proxies:
            #    print(i.element, i.tries)



        #sessions = await asyncio.gather(*[self.fetch_item(link, appid) for link in self.links])
        #for link in self.links:
        #    sessions.append(await self.fetch_item(link, appid=appid))
        #print('\n'*5)
        #print(sessions)

    async def check_proxy(self):
        proxy_list = self.proxies
        self.available_proxies = [proxy for proxy in await asyncio.gather(*[self.ping_proxy(proxy) for proxy in proxy_list]) if proxy]
        print(f'Рабочие прокси {self.available_proxies}')

    async def ping_proxy(self, proxy):
        async with aiohttp.ClientSession() as session:
            #print(f'Проверяю прокси {proxy.element}')
            response = await session.get(url='https://icanhazip.com/', proxy=proxy.element)
            #print(f'Проверил прокси {proxy.element}')
            if response.status == 200:
                return proxy
            else:
                return None


    def buy_item(self, some_id: str = '') -> dict:
        return dict()

    def hunt(self, market_name: str = '') -> dict:
        return dict()


bot = FastBot()
names = ['AK-47 | Inheritance (Field-Tested)', 'AK-47 | Asiimov (Field-Tested)', 'AK-47 | Redline (Field-Tested)', 'AK-47 | Phantom Disruptor (Field-Tested)', 'AWP | Atheris (Field-Tested)', 'M4A4 | Neo-Noir (Field-Tested)', 'USP-S | Cortex (Field-Tested)', 'AK-47 | Slate (Minimal Wear)', 'Desert Eagle | Trigger Discipline (Minimal Wear)', 'AK-47 | Slate (Well-Worn)', 'M4A4 | Desolate Space (Field-Tested)', 'AK-47 | Neon Revolution (Field-Tested)', 'AK-47 | Elite Build (Field-Tested)', 'AK-47 | The Empress (Field-Tested)', 'AK-47 | Legion of Anubis (Field-Tested)', 'Desert Eagle | Mecha Industries (Field-Tested)', 'AWP | Neo-Noir (Field-Tested)', 'AK-47 | Elite Build (Well-Worn)', 'AK-47 | Elite Build (Minimal Wear)', 'Desert Eagle | Printstream (Field-Tested)', 'M4A4 | Tooth Fairy (Field-Tested)', 'AK-47 | Point Disarray (Field-Tested)', 'AWP | Atheris (Minimal Wear)', 'Glock-18 | Vogue (Field-Tested)', 'AK-47 | Frontside Misty (Field-Tested)', 'AWP | Mortis (Field-Tested)', 'AWP | Fever Dream (Field-Tested)', 'AK-47 | Phantom Disruptor (Minimal Wear)', 'M4A1-S | Leaded Glass (Field-Tested)', 'Desert Eagle | Trigger Discipline (Factory New)', 'USP-S | Cyrex (Field-Tested)', 'Glock-18 | Water Elemental (Field-Tested)', 'AWP | Exoskeleton (Field-Tested)', 'AWP | Atheris (Well-Worn)', 'USP-S | Flashback (Field-Tested)', 'AWP | Exoskeleton (Well-Worn)', 'UMP-45 | Oscillator (Factory New)', 'AK-47 | Emerald Pinstripe (Field-Tested)', 'Desert Eagle | Mecha Industries (Minimal Wear)', 'SSG 08 | Acid Fade (Factory New)', 'USP-S | Cortex (Well-Worn)', 'StatTrak™ AK-47 | Slate (Field-Tested)', 'M4A1-S | Decimator (Field-Tested)', 'Desert Eagle | Light Rail (Field-Tested)', 'AWP | Mortis (Minimal Wear)', 'StatTrak™ AK-47 | Uncharted (Field-Tested)', 'AWP | PAW (Minimal Wear)', 'AK-47 | Rat Rod (Field-Tested)', 'M4A4 | Evil Daimyo (Field-Tested)', 'USP-S | Flashback (Minimal Wear)', 'USP-S | Cyrex (Minimal Wear)', 'AK-47 | Uncharted (Minimal Wear)', 'AK-47 | Emerald Pinstripe (Well-Worn)', 'AK-47 | Legion of Anubis (Minimal Wear)', 'Nova | Windblown (Factory New)', 'AWP | Worm God (Minimal Wear)', 'Desert Eagle | Conspiracy (Minimal Wear)', 'USP-S | Cortex (Battle-Scarred)', 'USP-S | Torque (Field-Tested)', 'AWP | Exoskeleton (Minimal Wear)', 'AK-47 | Elite Build (Battle-Scarred)', 'SG 553 | Anodized Navy (Factory New)', 'M4A1-S | Nightmare (Field-Tested)', 'Desert Eagle | Meteorite (Factory New)', 'Tec-9 | Fuel Injector (Field-Tested)', 'MP7 | Bloodsport (Field-Tested)', 'CZ75-Auto | Tuxedo (Minimal Wear)', 'AWP | Worm God (Field-Tested)', 'M4A1-S | Player Two (Field-Tested)', 'AK-47 | Slate (Factory New)', 'Five-SeveN | Silver Quartz (Minimal Wear)', 'M4A4 | Neo-Noir (Battle-Scarred)', 'XM1014 | Entombed (Minimal Wear)', 'M4A1-S | Nitro (Field-Tested)', 'MAC-10 | Disco Tech (Field-Tested)', 'M4A4 | Evil Daimyo (Minimal Wear)', 'M249 | O.S.I.P.R. (Factory New)', 'M4A4 | The Emperor (Field-Tested)', 'M4A1-S | Flashback (Field-Tested)', 'AUG | Chameleon (Field-Tested)', 'Glock-18 | Wasteland Rebel (Field-Tested)', 'AWP | PAW (Factory New)', 'Glock-18 | Vogue (Minimal Wear)', 'MAC-10 | Button Masher (Minimal Wear)', 'M4A4 | Tooth Fairy (Minimal Wear)', 'M4A1-S | Leaded Glass (Minimal Wear)', 'P2000 | Amber Fade (Minimal Wear)', 'MP7 | Anodized Navy (Factory New)', 'AK-47 | Emerald Pinstripe (Minimal Wear)', 'AWP | PAW (Field-Tested)', 'M4A1-S | VariCamo (Field-Tested)', 'Sawed-Off | Amber Fade (Factory New)', 'StatTrak™ M4A4 | Magnesium (Field-Tested)', 'Glock-18 | Candy Apple (Factory New)', 'Glock-18 | Water Elemental (Minimal Wear)', 'AWP | Phobos (Minimal Wear)', 'AWP | Pit Viper (Minimal Wear)', 'AWP | Atheris (Battle-Scarred)', 'USP-S | Torque (Factory New)', 'USP-S | Cortex (Minimal Wear)']
start = time.time()
asyncio.run(bot.parse_items(list_of_names=names[:10], appid='730'))

print(f"Все заняло {time.time() - start} секунд")