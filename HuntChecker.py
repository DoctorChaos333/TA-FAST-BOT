import asyncio
import datetime
import threading
import time
import traceback

import async_db
import cloudscraper
import fake_useragent
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

    if percent > 5:
        return True
    if result_dict.get(skin):
        for item in result_dict[skin]:
            if item[0] <= fl <= item[1]:
                return True
    if fl == 1:
        return True

    return False

async def update_base(id_, wear='[]', fl = 1, account_id = None, listing_price = None, default_price = None, buy_id = None, page_num=0, is_hunting=1, fee=0):
    async with async_db.Storage() as db:
        await db.update_hunting_temp(str(id_), str(wear), str(round(fl, 10)), account_id=account_id, price=listing_price, default_price=default_price, buy_id=buy_id, page_num=page_num, is_hunting=is_hunting, fee=fee)
        print(f"\rДобавил предмет {id_} в базу", end='', flush=True)


params = {
    'url': 'steam://rungame/730/76561202255233023/ csgo_econ_action_preview M5258603558030077991A39231682804D9241503133585526237'
}
url = 'https://floats.steaminventoryhelper.com/?url=steam://rungame/730/76561202255233023/+csgo_econ_action_preview%20M5258603558030077991A39231682804D9241503133585526237'


# print(scraper.get(url=url).json())
# input()
class HuntingParser:
    def __init__(self, items):
        self.scraper = None
        self.result_thread = None
        self.items = items

    async def parse_floats(self):

        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'firefox', 'platform': 'windows', 'mobile': False})

        min_count = min([len(self.items)])

        self.result_thread = [() for _ in range(min_count)]
        threads = [threading.Thread(target=self.th_parse_floats, args=value) for value in
                   self.items]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        time.sleep(5)

    def th_parse_floats(self, *args):
        for item in args:
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
                "X-Sih-Token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOjgyMzExLCJ0b2tlbiI6Ijg1OTk2ZTc5OTBmMTViZmM0YzQ1ZGQ4N2RmMjA0ZjBjYTA3YzhiOTkyYTA4NjMwZmM3Y2Y0ZDAyYjg2NjI0Mzk3NjM1ZjNmZjFjMmRjNDhmZDg5OGM4YzdiMGFhMDgyMzU0ZTBkZWZiZWUzNmEzYzQzZDE4NTU1ODM5NmM1NmE5IiwiZGF0YSI6bnVsbCwiaWF0IjoxNzI2NTgyMjc0LCJleHAiOjE3MjkxNzQyNzR9.5OFuloRm0Hw5-MZy46KJYUntYWsnLWSgjVLVSjpRV2A",
                "X-Sih-Version": "2.1.0"
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

                        print('Нашел float value', id_, wear, float_value)
                        if float_value:
                            fl = float_value
                        asyncio.run(update_base(id_, wear, fl, account_id, listing_price, default_price, buy_id, page_num, is_hunting, fee))
                    else:
                        print('NOT SUCCESS')
                else:
                    print(response.status_code, proxy)


async def fetch_hunt_temp():
    async with async_db.Storage() as db:
        items = await db.fetch_hunt_temp()
        return items

async def fetch_proxy():
    async with async_db.Storage() as db:
        proxies = await db.fetch_hunting_proxies()
        return proxies



def main():
    while True:
        items = asyncio.run(fetch_hunt_temp())
        proxies = asyncio.run(fetch_proxy())
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

        try:
            if items and proxies:
                print('ВСЕГО ПОТОКОВ', len(proxies_pack))
                float_parser = HuntingParser(proxies_pack)
                asyncio.run(float_parser.parse_floats())
            else:
                print(f'\r[{datetime.datetime.now()}] ПОКА НЕТ ПРЕДМЕТОВ ДЛЯ ПАРСИНГА ФЛОТОВ', flush=True, end='')
                time.sleep(0.3)
        except:
            print(traceback.format_exc())
            time.sleep(10)


if __name__ == '__main__':
    main()
