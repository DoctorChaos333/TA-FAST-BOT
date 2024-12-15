import asyncio
import datetime
import time
import traceback
import requests
import threading
import fake_useragent
import cloudscraper
import async_db
import pandas as pd
import os
import time
import gc
import re

df = pd.read_excel('База предметов.xlsx')
result_dict = dict(zip(df['Предмет'], df['Какой float ценится']))
for k in result_dict:
    v = result_dict[k]
    v1 = [[float(num) for num in item.split('-')] for item in v.split(' | ')]
    result_dict[k] = v1


def matches_mask(fl):
    # Преобразуем fl в строку без изменения его точности
    fl_str = str(fl)

    # Определяем регулярные выражения для масок
    patterns = [
        r"^0\.\d{1,2}000$",  # 0.xy000 или 0.x000 или 0.000
        r"^0\.(\d)\1{2,}$",  # 0.yyyy или 0.xxx (несколько одинаковых цифр с первого-второго знака)
    ]

    # Проверяем fl_str на соответствие хотя бы одному из шаблонов
    for pattern in patterns:
        if re.match(pattern, fl_str):
            return True
    return False


def is_rarity(skin, fl, percent):
    fl = float(fl)
    percent = float(percent)

    if percent > 5:
        return True
    if result_dict.get(skin):
        for item in result_dict[skin]:
            if item[0] <= fl <= item[1] or matches_mask(fl):
                return True
    if fl == 1:
        return True

    return False


params = {
    'url': 'steam://rungame/730/76561202255233023/ csgo_econ_action_preview M5258603558030077991A39231682804D9241503133585526237'
}
url = 'https://floats.steaminventoryhelper.com/?url=steam://rungame/730/76561202255233023/+csgo_econ_action_preview%20M5258603558030077991A39231682804D9241503133585526237'


# print(scraper.get(url=url).json())
# input()
class FloatParser:
    def __init__(self, items, proxies):
        self.items = items
        self.proxies = []
        for proxy in proxies:
            self.proxies.append(
                {'https': proxy,
                 'http': proxy}
            )
        self.items_to_update = []
        self.last_ts = datetime.datetime.now()
        self.scraper = None

    def parse_floats(self):
        if self.scraper:
            self.scraper.close()
            del self.scraper

        with cloudscraper.create_scraper(
            browser={'browser': 'firefox', 'platform': 'windows', 'mobile': False}) as self.scraper:
            self.items = [i for i in self.items if i['percent'] > -10]

            while self.items:
                gc.collect()
                time.sleep(7)
                min_count = min([len(self.items), len(self.proxies)])
                proxy_thread = [self.proxies[i] for i in range(min_count)]
                item_thread = [self.items.pop(0) for _ in range(min_count)]

                self.parsing_result = {
                    'all': len(item_thread),
                    'not_parsed': 0,
                    'not_float': 0,
                    'low_percent': 0,
                    'new_percent_lower': 0,
                    'bad_stickers': [],
                    'added': 0,
                }

                self.result_thread = [() for _ in range(min_count)]
                threads = [threading.Thread(target=self.th_parse_floats, args=(item_thread[i], proxy_thread[i], i)) for i in
                           range(min_count)]

                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

                self.result_thread = [element for element in self.result_thread if len(element) > 0]

                # print(*self.items_to_update, sep='\n')
                print()
                print('УДАЛЮ', len(self.items_to_update))


                showed_dict = {}
                for item in self.result_thread:
                    if item[1] not in showed_dict:
                        showed_dict[item[1]] = 1
                    else:
                        showed_dict[item[1]] += 1



                asyncio.run(smthmany(self.result_thread, name='spam_profit'))

                self.parsing_result['added'] = len(self.result_thread)

                asyncio.run(log('parse_floats', '', f'Результат: {self.parsing_result},'))

                if showed_dict:
                    asyncio.run(dump_statistics_showed(showed_dict))

                if len(self.items_to_update) > 1:
                    asyncio.run(delete_ids(self.items_to_update))
                    self.items_to_update = []

        if self.scraper:
            self.scraper.close()
            del self.scraper
        gc.collect()

    def th_parse_floats(self, item: dict, proxy: dict, idx: int):

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
            "X-Sih-Token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOjM4MDI2LCJ0b2tlbiI6IjdkZjIwMTFkZjQ3YTI1Yjk1M2VkNzBlNTRlNjYxMjc2NTkzZTE1OTE4OTA2NjZkYTI5MDY5OTliMjgyNjQwMThmY2JjNTkyMzliMjExNzUzNzg0MTk0ZTY3NTA4MTM5ODRjMDE2ZDhhNWNlODRlMmRiMGY0N2NmNDk5NjQzMTNhIiwiZGF0YSI6bnVsbCwiaWF0IjoxNzMxODg0MDcwLCJleHAiOjE3MzQ0NzYwNzB9.yFYLaNo1hcgnqjRdVtEhr9DE8g2N10E1uQWz1kE501c",
            "X-Sih-Version": "2.1.13"
        }
        # steam://rungame/730/76561202255233023/ csgo_econ_action_preview M5269862557101848699A39457149428D478659805249054517
        fl_url = f"https://floats.steaminventoryhelper.com/?url={link.replace('20M%', '20M').replace('%A%', 'A').replace('%D', 'D')}"

        buy_id = item['buy_id']
        skin = item['skin']
        id_ = item['id']
        listing_price = item['listing_price']
        default_price = item['default_price']
        steam_without_fee = item['steam_without_fee']
        sticker = item['sticker']
        url = item['url']
        ts = self.last_ts + datetime.timedelta(seconds=1)
        self.last_ts += datetime.timedelta(seconds=1)
        wear = item['wear']
        sticker_slot = item['sticker_slot']
        sticker_price = item['sticker_price']
        profit = item['profit']
        # old_profit = float(profit)
        percent = item['percent']
        page_num = item['page_num']

        print(f'\rИщу {skin}', flush=True, end='')
        if item['fl'] != '1':
            fl = item['fl']
            if is_rarity(skin, fl, percent):
                # print(skin, 'подходит под параметры')
                self.result_thread[idx] = (
                    str(buy_id), str(skin), str(id_), str(listing_price), str(default_price), str(steam_without_fee),
                    sticker, url,
                    str(ts), str(wear), str(sticker_slot), str(sticker_price), str(profit), str(percent), str(fl),
                    page_num)
        not_response = False
        bad_stickers = False
        try:
            response = self.scraper.get(fl_url, proxies=proxy, headers=headers, verify=True, timeout=5)
        except:
            print('Пока такого скина нет в бд SIH')
            #
            self.parsing_result['not_parsed'] += 1
            print(157, proxy)
            self.items_to_update.append(id_)
            not_response = True
        finally:
            gc.collect()
            if not_response:
                return 0



        if response.status_code == 200:
            rs_json = response.json()
            if rs_json.get('success'):
                item_info = rs_json['iteminfo']

                float_value = item_info.get('floatvalue')
                full_item_name = item_info.get('full_item_name')
                stickers = item_info['stickers']

                if stickers:
                    sticker = eval(sticker)
                    new_sticker_price = eval(sticker_price)
                    sticker_slot = eval(sticker_slot)
                    sticker_price = eval(sticker_price)
                    api_wear = [[st['name'], st['wear']] for st in stickers]
                    profit = float(profit)
                    old_percent = float(percent)

                    listing_price = float(listing_price)
                    default_price = float(default_price)
                    wear = eval(wear)
                    wear_dict = dict()
                    for name, w in api_wear:
                        if 'Sticker | ' + name not in wear_dict:
                            wear_dict['Sticker | ' + name] = [w]
                        else:
                            wear_dict['Sticker | ' + name].append(w)
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
                    if len(wear) == len(sticker_price) == len(sticker) == len(sticker_slot):
                        pass
                    else:
                        print('\n' * 5)
                        print('Как-то не так все вышло')
                        print('sticker', sticker, len(sticker))
                        print('api_wear', api_wear, len(api_wear))
                        print('wear', wear, len(wear))
                        print('price', sticker_price, len(sticker_price))
                        print('slot', sticker_slot, len(sticker_slot))
                        print('\n' * 5)
                        input()
                        return 0

                    sticker_addition_price = 0
                    profit = 0
                    percent = 0

                    for j in range(len(sticker)):
                        try:
                            st_price = float(sticker_price[j][1])
                        except:
                            return 0
                        # if wear[j][1] != 0 and sticker.count(sticker[j]) == 1:
                        #
                        #    if st_price > 30000:
                        #        profit -= (st_price * 0.045)
                        #    elif 10000 < st_price < 30000:
                        #        profit -= (st_price * 0.085)
                        #    elif st_price < 10000:
                        #        profit -= (st_price * 0.095)

                        if sticker.count(sticker[j]) == 2:
                            sticker_addition_price += st_price * 0.2
                        elif sticker.count(sticker[j]) == 3:
                            sticker_addition_price += st_price * 0.25
                        elif sticker.count(sticker[j]) > 3:
                            sticker_addition_price += st_price * 0.3
                        elif sticker.count(sticker[j]) == 1 and wear[j][1] == 0:
                            if st_price > 30000:
                                sticker_addition_price += (st_price * 0.045)
                            elif 10000 < st_price < 30000:
                                sticker_addition_price += (st_price * 0.085)
                            elif st_price < 10000:
                                sticker_addition_price += (st_price * 0.095)
                        elif sticker.count(sticker[j]) == 1 and wear[j][1] > 0:
                            bad_stickers = True


                    profit = (default_price * 0.87 + sticker_addition_price - listing_price)

                    profit = round(profit, 2)
                    # print(sticker_price)
                    percent = round(profit / listing_price * 100, 2)

                    if percent < round(old_percent * 0.9, 2):
                        self.parsing_result['new_percent_lower'] += 1

                    #if bad_stickers:
                    #    self.parsing_result['bad_stickers'].append([skin, buy_id, listing_price, fl_url, wear, api_wear])

                    wear = str(wear)
                    sticker = str(sticker)
                    sticker_price = str(sticker_price)
                    sticker_slot = str(sticker_slot)

                # print(skin, float_value, profit, old_profit)
                # if old_profit > profit:
                #    print(buy_id, wear)
                if float_value and percent > -10:
                    fl = float_value

                    self.result_thread[idx] = (
                        buy_id, skin, id_, listing_price, default_price, steam_without_fee, sticker, url, ts, wear,
                        sticker_slot, sticker_price, profit, percent, fl, page_num)
                elif not float_value:
                    self.parsing_result['not_float'] += 1
                    self.items_to_update.append(id_)
                elif percent < -10:
                    self.parsing_result['low_percent'] += 1
                    self.items_to_update.append(id_)
                else:
                    self.items_to_update.append(id_)
            else:
                print('NOT SUCCESS')
        else:
            print('Такой response_status_code', response.status_code, proxy['http'])


async def fetch_temp():
    async with async_db.Storage() as db:
        return await db.fetch_temp()

async def smthmany(args, name):
    async with async_db.Storage() as db:
        await db.smthmany(args, name)

async def delete_ids(args):
    async with async_db.Storage() as db:
        await db.delete_ids(args)

async def dump_statistics_showed(args: dict):
    data = []
    for skin, showed in args.items():
        data.append((skin, showed))
    async with async_db.Storage() as db:
        await db.dump_statistics_showed(data)


async def log(item, proxy_used, message, thread_id=None):
    """Асинхронная функция для записи лога."""
    async with async_db.Storage() as db:
        await db.add_log_entry(item, proxy_used, message, thread_id, table_name='log_entries_FC')

def main():

    #loop = asyncio.new_event_loop()
    #asyncio.set_event_loop(loop)
    proxies = []
    with open('proxies_for_float.txt', 'r', encoding='utf-8') as file:
        for line in file.readlines():
            proxies.append('http://' + line.strip().split(':')[2] + ':' + line.strip().split(':')[3] + '@' + \
                    line.strip().split(':')[0] + ':' + line.strip().split(':')[1])
    if True:
        print('Получаю ссылки...')
        while True:
            time.sleep(1)
            try:
                items = asyncio.run(fetch_temp())
            except:
                print(traceback.format_exc())
                continue
            print('Сейчас буду обрабатывать')
            try:
                if items:
                    asyncio.run(log('main', '', f'Получил ссылки: {len(items)} штук'))

                    float_parser = FloatParser(items.copy(), proxies=proxies)
                    if float_parser.scraper:
                        float_parser.scraper.close()
                    del items
                    float_parser.parse_floats()

                else:
                    print(f'\r[{datetime.datetime.now()}] ПОКА НЕТ ПРЕДМЕТОВ', flush=True, end='')

            except:
                print(traceback.format_exc())
                time.sleep(10)


if __name__ == '__main__':
    main()
