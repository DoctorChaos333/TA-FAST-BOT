import asyncio
import json
import time

import aiohttp
import requests
import numpy as np
import fake_useragent
import os


class CurrencyParser:
    async def get_currency_rates(self, proxy, currency_values):
        url = "https://steamcommunity.com/market/priceoverview/"


        await asyncio.sleep(3.5)



        headers = {
            'User-Agent': fake_useragent.UserAgent().random
        }
        currs = dict()
        for currency_value in currency_values:
            await asyncio.sleep(3.5)
            params = {
                'country': 'PL',
                'currency': currency_value,
                'appid': '440',
                'market_hash_name': 'Mann Co. Supply Crate Key'
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params, proxy=proxy) as response:
                    print(f"\rПаршу валюту {Currency()[currency_value]}", flush=True, end='')
                    if response.status == 200:
                        response = await response.json()
                        median_price_un = response['median_price']
                        median_price = int(''.join([ch for ch in median_price_un if ch.isdigit()])) / 100
                        currs[currency_value] = median_price
        return currs

async def main():

    if False:
        with open('proxies.txt', 'r', encoding='utf-8') as file:
            proxies = ['http://' + line.strip().split(':')[2] + ':' + line.strip().split(':')[3] + '@' +
                        line.strip().split(':')[
                            0] + ':' + line.strip().split(':')[1] for line in file.readlines()]
            link_threads = [[] for _ in range(len(proxies))]

            for i in range(1, 48):
                idx = (i - 1) % len(proxies)

                link_threads[idx].append(i)
            CurrencyParser().counter = 0
            tasks = [asyncio.create_task(CurrencyParser().get_currency_rates(proxies[i], link_threads[i])) for i in range(len(link_threads))]

            responses = await asyncio.gather(*tasks)

            currencies = dict()
            for i in responses:
                currencies |= i

            print()

            with open('currencies.json', 'r', encoding='utf-8') as file:
                old_dct = json.load(fp=file)
                usd_curs = currencies[1]

                new_dct = {Currency()[i[0]]: round(i[1] / usd_curs, 2) for i in currencies.items()}
                old_dct.update(new_dct)

            with open('currencies.json', 'w', encoding='utf-8') as file:
                json.dump(new_dct, fp=file)
    else:
        with open('currencies.json', 'r', encoding='utf-8') as file:
            new_dct = json.load(fp=file)
    return new_dct


class Currency():


    USD = 1
    GBP = 2
    EURO = 3
    CHF = 4
    RUB = 5
    PLN = 6
    BRL = 7
    JPY = 8
    NOK = 9
    IDR = 10
    MYR = 11
    PHP = 12
    SGD = 13
    THB = 14
    VND = 15
    KRW = 16
    TRY = 17
    UAH = 18
    MXN = 19
    CAD = 20
    AUD = 21
    NZD = 22
    CNY = 23
    INR = 24
    CLP = 25
    PEN = 26
    COP = 27
    ZAR = 28
    HKD = 29
    TWD = 30
    SAR = 31
    AED = 32
    SEK = 33
    ARS = 34
    ILS = 35
    BYN = 36
    KZT = 37
    KWD = 38
    QAR = 39
    CRC = 40
    UYU = 41
    BGN = 42
    HRK = 43
    CZK = 44
    DKK = 45
    HUF = 46
    RON = 47

    def __init__(self):
        self.dct = {'1': 'USD', '2': 'GBP', '3': 'EURO', '4': 'CHF', '5': 'RUB', '6': 'PLN', '7': 'BRL', '8': 'JPY',
                    '9': 'NOK',
                    '10': 'IDR', '11': 'MYR', '12': 'PHP', '13': 'SGD', '14': 'THB', '15': 'VND', '16': 'KRW',
                    '17': 'TRY',
                    '18': 'UAH', '19': 'MXN', '20': 'CAD', '21': 'AUD', '22': 'NZD', '23': 'CNY', '24': 'INR',
                    '25': 'CLP',
                    '26': 'PEN', '27': 'COP', '28': 'ZAR', '29': 'HKD', '30': 'TWD', '31': 'SAR', '32': 'AED',
                    '33': 'SEK',
                    '34': 'ARS', '35': 'ILS', '36': 'BYN', '37': 'KZT', '38': 'KWD', '39': 'QAR', '40': 'CRC',
                    '41': 'UYU',
                    '42': 'BGN', '43': 'HRK', '44': 'CZK', '45': 'DKK', '46': 'HUF', '47': 'RON',
                    'USD': '1', 'GBP': '2', 'EURO': '3', 'CHF': '4', 'RUB': '5', 'PLN': '6', 'BRL': '7', 'JPY': '8', 'NOK': '9',
                    'IDR': '10', 'MYR': '11', 'PHP': '12', 'SGD': '13', 'THB': '14', 'VND': '15', 'KRW': '16', 'TRY': '17',
                    'UAH': '18', 'MXN': '19', 'CAD': '20', 'AUD': '21', 'NZD': '22', 'CNY': '23', 'INR': '24', 'CLP': '25',
                    'PEN': '26', 'COP': '27', 'ZAR': '28', 'HKD': '29', 'TWD': '30', 'SAR': '31', 'AED': '32', 'SEK': '33',
                    'ARS': '34', 'ILS': '35', 'BYN': '36', 'KZT': '37', 'KWD': '38', 'QAR': '39', 'CRC': '40', 'UYU': '41',
                    'BGN': '42', 'HRK': '43', 'CZK': '44', 'DKK': '45', 'HUF': '46', 'RON': '47'}



    def __len__(self):
        return len(self.dct)

    def __getitem__(self, index: str | int):
        return self.dct[str(index)]