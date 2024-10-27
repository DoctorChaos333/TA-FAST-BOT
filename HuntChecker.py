import asyncio
import datetime
import time
import traceback
from asyncio import AbstractEventLoop
import tracemalloc
import async_db
import cloudscraper
import fake_useragent
import pandas as pd
import gc
import sys
import threading
import importlib
from concurrent.futures import ThreadPoolExecutor

df = pd.read_excel('База предметов.xlsx')
result_dict = dict(zip(df['Предмет'], df['Какой float ценится']))
for k in result_dict:
    v = result_dict[k]
    v1 = [[float(num) for num in item.split('-')] for item in v.split(' | ')]
    result_dict[k] = v1
del v
del v1
del df
del k


def is_rarity(skin, fl, percent):
    print(22)
    fl = float(fl)
    percent = float(percent)

    if percent > 5:
        return True
    if result_dict.get(skin):
        for item in result_dict[skin]:
            if item[0] <= fl <= item[1]:
                return True
    if fl == 1:
        return True

    return False
#id_, wear='[]', fl = 1, account_id = None, listing_price = None, default_price = None, buy_id = None, page_num=0, is_hunting=1, fee=0
async def update_base(args):
    async with async_db.Storage() as db:
        await db.update_hunting_temp(args)
        print(f"\rДобавил предмет {[arg[0] for arg in args]} в базу", end='', flush=True)
    del db
    del args
    return None

tasks_ids = set()

class HuntingParser:
    def __init__(self, items):
        self.result_thread = None
        self.items = items
        self.items_to_update = []
        self.update_counter = 0
        self.scraper = None

    def parse_floats(self):

        with cloudscraper.create_scraper(
            browser={'browser': 'firefox', 'platform': 'windows', 'mobile': False}) as scraper:
            self.scraper = scraper

            min_count = min([len(self.items)])

            self.result_thread = [() for _ in range(min_count)]

            threads = [threading.Thread(target=self.th_parse_floats, args=(value,)) for value in
                       self.items]

            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        """with ThreadPoolExecutor(max_workers=len(self.items), thread_name_prefix='___71___') as executor:
            futures = [executor.submit(self.th_parse_floats, value) for value in self.items]
            for future in futures:
                future.result()
            executor.shutdown(wait=True)
            gc.collect()"""
        #del executor
        self.scraper = None

        time.sleep(5)

    def th_parse_floats(self, args):
        for item in args:
            gc.collect()
            link = item['market_actions']
            headers = {
                "authority": "floats.steaminventoryhelper.com",
                "method": "GET",
                "path": f"/?url={link.replace('20M%', '20M').replace('%A%', 'A').replace('%D', 'D')}",
                "scheme": "https",
                "Accept": "/",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
                "Pragma": "no-cache",
                "Priority": "u=1, i",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "none",
                "User-Agent": fake_useragent.UserAgent().firefox,
                "X-Sih-Token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOjM4MDI2LCJ0b2tlbiI6IjdkZjIwMTFkZjQ3YTI1Yjk1M2VkNzBlNTRlNjYxMjc2NTkzZTE1OTE4OTA2NjZkYTI5MDY5OTliMjgyNjQwMThmY2JjNTkyMzliMjExNzUzNzg0MTk0ZTY3NTA4MTM5ODRjMDE2ZDhhNWNlODRlMmRiMGY0N2NmNDk5NjQzMTNhIiwiZGF0YSI6bnVsbCwiaWF0IjoxNzI5MjA4NjQ4LCJleHAiOjE3MzE4MDA2NDh9.WqbYPUI2Z1Bo_8IW7l0wsp5LxCg38laeHWNwRWP24Lk",
                "X-Sih-Version": "2.1.8"
            }
            # steam://rungame/730/76561202255233023/ csgo_econ_action_preview M5269862557101848699A39457149428D478659805249054517
            fl_url = f"https://floats.steaminventoryhelper.com/?url={link.replace('20M%', '20M').replace('%A%', 'A').replace('%D', 'D')}"

            buy_id = item['buy_id']
            id_ = item['id']
            wear = item['wear']
            account_id = item['account_id']
            default_price = item['default_price']
            listing_price = item['listing_price']
            page_num = item['page_num']
            is_hunting = 1
            fee = item['fee']

            proxy = {
                'http': item['proxy'],
                'https': item['proxy']
            }

            print(f"[{datetime.datetime.now()}] Ищу {id_} {item['proxy']}")
            not_sih = False
            if True:
                time.sleep(3.5)
                try:
                    response = self.scraper.get(fl_url, proxies=proxy, headers=headers, verify=True, timeout=5)
                except:
                    print('Пока такого скина нет в бд SIH')
                    try:
                        not_sih = True
                        headers = {
                            'Accept': 'application/json, text/plain, */*',
                            'Referer': 'https://csfloat.com/',
                            'Sec-Ch-UA': '"Chromium";v="128", "Not;A=Brand";v="24", "Microsoft Edge";v="128"',
                            'Sec-Ch-UA-Mobile': '?0',
                            'Sec-Ch-UA-Platform': '"Windows"',
                            'Origin': 'https://csfloat.com',
                            "User-Agent": fake_useragent.UserAgent().firefox,
                        }
                        fl_url = f"https://api.csfloat.com/?url={link.replace('20M%', '20M').replace('%A%', 'A').replace('%D', 'D')}"
                        response = self.scraper.get(fl_url, proxies=proxy, headers=headers, verify=True, timeout=5)
                        print('Но нашел в CSFLOAT')
                    except:
                        print(traceback.format_exc())
                        print('CS FLOAT тоже не нашел')
                        continue
                if response.status_code == 200:
                    rs_json = response.json()
                    if rs_json.get('success') or not_sih:
                        item_info = rs_json['iteminfo']

                        float_value = item_info.get('floatvalue')
                        stickers = item_info.get('stickers')

                        if stickers and not not_sih:
                            api_wear = [[st['name'], st['wear']] for st in stickers]

                            wear = eval(wear)
                            wear_dict = dict()
                            for name, w in api_wear:
                                if 'Sticker | ' + name not in wear_dict:
                                    wear_dict['Sticker | ' + name] = [w]
                                else:
                                    wear_dict['Sticker | ' + name].append(w)
                            if '0.' in str(wear_dict):
                                print('ТОЧКАААААААААААААА', wear_dict)
                            for i in range(len(wear)):
                                try:
                                    name, _ = wear[i]
                                except:
                                    print('\n' * 5)
                                    print('ОШИБКА')
                                    print(wear)
                                    print('\n' * 5)
                                if name in wear_dict:
                                    if wear_dict.get(name):
                                        wear[i][1] = wear_dict[name].pop(0)
                                    else:
                                        wear[i][1] = 0
                            del api_wear
                            del wear_dict

                        print('Нашел float value', id_, wear, float_value)
                        if float_value:
                            fl = float_value
                        self.items_to_update.append([id_, wear, fl, account_id, listing_price, default_price, buy_id, page_num, is_hunting, fee])

                        if len(self.items_to_update) >= len(self.items) or self.items[-1] == []:
                            self.update_counter = 0
                            temp_items = self.items_to_update.copy()
                            self.items_to_update = []
                            asyncio.run(update_base(temp_items))
                            time.sleep(0.3)
                            #del new_loop
                        else:
                            self.update_counter += 1
                    else:
                        print('NOT SUCCESS')
                else:
                    print(response.status_code, proxy)
        gc.collect()
        if args:
            del item
            del response
            del rs_json
            del item_info
            del float_value
            del stickers
            del buy_id
            del args

async def fetch_hunt_temp():
    async with async_db.Storage() as db:
        items = await db.fetch_hunt_temp()
        return items

async def fetch_proxy():
    async with async_db.Storage() as db:
        proxies = await db.fetch_hunting_proxies()
        return proxies



def main():
    tracemalloc.start()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    old_modules = sys.modules.copy()


    while True:
        gc.collect()
        #snapshot = tracemalloc.take_snapshot()  # Снимок состояния памяти
        #top_stats = snapshot.statistics('lineno')  # Статистика по строкам кода
        #print(top_stats[:10])
        #line = '\n'.join(str(i) for i in top_stats[:10])
        #print(f'\r{line}', end='', flush=True)
        gc.collect()
        time.sleep(1)
        items = loop.run_until_complete(fetch_hunt_temp())
        proxies = loop.run_until_complete(fetch_proxy())
        proxies_pack = [[] for i in range(len(proxies))]
        new_items_values = []
        for item in items.values():
            for j in item:
                new_items_values.append(j)



        for i in range(len(new_items_values)):

            idx = i % len(proxies)
            value = new_items_values[i]
            value['proxy'] = proxies[idx]
            proxies_pack[idx].append(value)

        if items:
            del item
            del j

        try:

            if items and proxies:
                items.clear()
                proxies.clear()
                print('ВСЕГО ПОТОКОВ', len(proxies_pack))
                float_parser = HuntingParser(proxies_pack)

                float_parser.parse_floats()

                float_parser.items_to_update.clear()
                float_parser.result_thread = []
                float_parser.items = []
                del float_parser

            else:
                print(f'[{datetime.datetime.now()}] ПОКА НЕТ ПРЕДМЕТОВ ДЛЯ ПАРСИНГА ФЛОТОВ')
                time.sleep(0.3)

            del items
            del proxies
            del new_items_values
            del proxies_pack
            gc.collect()
            new_modules = sys.modules.copy()
            for key in new_modules:
                if not key in old_modules:
                    del sys.modules[key]

        except:
            print(traceback.format_exc())
            time.sleep(10)

if __name__ == '__main__':
    main()