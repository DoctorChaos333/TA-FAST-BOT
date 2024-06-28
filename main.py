import datetime
import pickle
import time

import pymysql
import traceback
import requests
import steampy.client
import steampy.exceptions
from steampy.client import SteamClient, Asset
from steampy.guard import generate_one_time_code
from steampy.models import GameOptions
from flask import Flask, make_response, request
from flask_restful import Api, Resource
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

def encrypt(key, data):
    iv = get_random_bytes(AES.block_size)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded_data = pad(data, AES.block_size)
    encrypted_data = cipher.encrypt(padded_data)
    return iv + encrypted_data


def decrypt(key, encrypted_data):
    iv = encrypted_data[:AES.block_size]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted_data = cipher.decrypt(encrypted_data[AES.block_size:])
    unpadded_data = unpad(decrypted_data, AES.block_size)
    return unpadded_data

with open('key.bin', 'rb') as file:
    key = file.read()


def AddAccount(account_data: dict) -> dict:
    try:
        connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            steam_id_ = account_data['steam_id']
            login_ = account_data['login']
            password_ = account_data['password']
            shared_secret_ = account_data['shared_secret']
            identity_secret_ = account_data['identity_secret']
            api_key = account_data['api_key']
            proxy = account_data.get('proxy')
            bots = account_data.get('bots')



            #Проверка, есть ли такой аккаунт вообще
            cmd = f"SELECT * FROM user WHERE login = '{login_}'"
            cursor.execute(cmd)
            if cursor.fetchone():
                return {'success': 0, 'message': 'Такой аккаунт уже есть в базе данных'}

            values = [item for item in
                      [steam_id_, login_, password_, shared_secret_, identity_secret_, api_key, proxy, bots] if item]
            columns = ['steam_id', 'login', 'password', 'shared_secret', 'identity_secret', 'api_key']

            if proxy:
                columns.append('proxy')
            if bots:
                columns.append('bots')

            columns_str = (', ').join(columns)
            values_str = ("', '").join(values)

            cmd1 = f"INSERT INTO `user` ({columns_str}) VALUES ('{values_str}')"
            cursor.execute(cmd1)
            if proxy:
                AddProxy(proxy_=proxy, type_='private', accounts_=login_)
            connection.commit()
        return {'success': 1, 'message': 'Удалось добавить аккаунт'}
    except:
        print(traceback.format_exc())
        return {'success': 0, 'message': traceback.format_exc()}


def RewriteAccount(account_data: dict) -> dict:
    try:
        connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            steam_id = account_data['steam_id']
            login = account_data['login']
            password = account_data['password']
            shared_secret = account_data['shared_secret']
            identity_secret = account_data['identity_secret']
            api_key = account_data['api_key']
            proxy = account_data.get('proxy')
            bots = account_data.get('bots')

            cmd = f"SELECT * FROM user WHERE login = '{login}'"
            cursor.execute(cmd)
            if not cursor.fetchone():
                return {'success': 0, 'message': 'Такого аккаунта нет в базе данных'}

            columns = ['steam_id', 'login', 'password', 'shared_secret', 'identity_secret', 'api_key', 'proxy', 'bots']
            lst = []
            for item in columns:
                if eval(item):
                    lst.append(f"{item} = \'{eval(item)}\'")
            cmd = "UPDATE user SET " + (', ').join(lst) + f" WHERE login = '{login}'"

            cursor.execute(cmd)
            connection.commit()
            return {'success': 1, 'message': 'Удалось перезаписать аккаунт'}
    except:
        print(traceback.format_exc())
        return {'success': 0, 'message': traceback.format_exc()}


def DelAccount(account) -> dict:
    try:
        connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            #Проверяю, есть ли аккаунт в базе
            cmd = f"SELECT * FROM user WHERE login = '{account}'"
            cursor.execute(cmd)
            if not cursor.fetchone():
                return {'success': 0, 'message': 'Такого аккаунта нет в базе данных'}

            #Удаляю аккаунт
            cmd = f"DELETE from user WHERE login = '{account}'"
            cursor.execute(cmd)

            #Заново переобозначаю индексы в таблице
            cmd = f"ALTER TABLE `user` AUTO_INCREMENT = 1"
            cursor.execute(cmd)
        connection.commit()
        return {'success': 1, 'message': 'Удалось удалить аккаунт'}
    except:
        print(traceback.format_exc())
        return {'success': 0, 'message': traceback.format_exc()}



def SteamEndSSN(login_: str) -> dict:
    connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            cmd = f"SELECT session FROM user WHERE login = '{login_}'"
            cursor.execute(cmd)
            account = cursor.fetchone()
            if not account:
                return {'success': 0, 'message': 'Такого аккаунта в базе данных нет'}
            if not account['session']:
                return {'success': 1, 'message': 'Сессия на данном аккаунте не была запущена'}
            response = SteamCreateSSN(login_)
            success = response['success']
            steam_client = pickle.loads(response['result']['session'])

            if success and steam_client.is_session_alive():
                steam_client.logout()
        return {'success': 1, 'message': 'Сессия завершена'}
    except:
        print(traceback.format_exc())
        return {'success': 0, 'message': traceback.format_exc()}


def ConfirmSteamAll(login_: str) -> dict:

    response = SteamCreateSSN(login_=login_)
    if not response['success']:
        return response
    steam_client = pickle.loads(response['result']['session'])
    listings = steam_client.market.get_my_market_listings().get('sell_listings')
    confirmed_items = []
    if listings:
        for id in listings:
            try:
                conf_needed = listings[id]['need_confirmation']
                if conf_needed:
                    market_name = listings[id]['description']['market_name']
                    item_id = listings[id]['description']['id']
                    success = steam_client.market._confirm_sell_listing(asset_id=id, item_id=item_id)
                    if success.get('success'):
                        confirmed_items.append(market_name)
            except steampy.exceptions.ConfirmationExpected:
                return {'success': 0, 'message': 'Не удалось подтвердить выставление. Попробуйте чуть позже'}
    if confirmed_items:
        return {'success': 1, 'result': confirmed_items}
    else:
        return {'success': 1, 'message': 'Предмет не выставлен или не требует подтверждения на выставление.'}


def ConfirmSteamOne(login_: str, item_id_: str) -> dict:
    response = SteamCreateSSN(login_=login_)
    if not response['success']:
        return response
    steam_client = pickle.loads(response['result']['session'])
    listings = steam_client.market.get_my_market_listings().get('sell_listings')
    if listings:
        for id in listings:
            item_id = listings[id]['description']['id']
            conf_needed = listings[id]['need_confirmation']
            if item_id == item_id_ and conf_needed:
                success = steam_client.market._confirm_sell_listing(asset_id=id, item_id=item_id)
                if success.get('success'):
                    return {'success': 1, 'message': 'Предмет подтвержден'}
                else:
                    return {'success': 0, 'message': 'Не удалось подтвердить выставление предмета'}
            elif item_id == item_id_ and not conf_needed:
                return {'success': 1, 'message': 'Предмет не требует подтверждения'}
        else:
            return {'success': 0, 'message': 'Нет предметов для подтверждения с таким id'}
    else:
        return {'success': 0, 'message': 'Нет лотов для подтверждения'}

def SendSteamOne(login_: str, id_: str, game: str, trade_offer_url: str) -> dict:
    try:
        response = SteamCreateSSN(login_=login_)
        if not response['success']:
            return response
        steam_client = pickle.loads(response['result']['session'])
        try:
            game = eval(f"GameOptions.{game.upper()}")
        except:
            return {'success': 0, 'message': 'Такой игры в списке нет. Возможные игры: TF2, CS, PUBG, RUST, DOTA2, STEAM'}
        items_from_me = [Asset(id_, game)]
        response = steam_client.make_offer_with_url(items_from_me=items_from_me, items_from_them=[], trade_offer_url=trade_offer_url)
        if response:
            return {'success': 1, 'message': 'Предмет передан'}
    except:
        return {'success': 0, 'message': 'Не удалось передать предмет или его нет в инвентаре'}


def SendSteamAll(login_: str, game: str, trade_offer_url: str) -> str | dict:
    try:
        response = SteamCreateSSN(login_=login_)
        if not response['success']:
            return response
        steam_client = pickle.loads(response['result']['session'])
        try:
            game = eval(f"GameOptions.{game.upper()}")
        except:
            return {'success': 0,
                    'message': 'Такой игры в списке нет. Возможные игры: TF2, CS, PUBG, RUST, DOTA2, STEAM'}
        my_inv = steam_client.get_my_inventory(game=game)
        ids = [my_inv[asset_id]['id'] for asset_id in my_inv if my_inv[asset_id]['tradable']]
        items_from_me = [Asset(id_, game) for id_ in ids]
        response = steam_client.make_offer_with_url(items_from_me=items_from_me, items_from_them=[],
                                                    trade_offer_url=trade_offer_url)
        if response:
            print(response)
            return {'success': 1, 'message': 'Предметы переданы'}
    except:
        return {'success': 0, 'message': traceback.format_exc()}
    return {'success': 0, 'message': 'Не удалось передать предмет или его нет в инвентаре'}


def GenerateCode(account) -> dict:
    try:
        # Здесь я должен из базы данных достать shared_secret
        connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            cmd = f"SELECT shared_secret FROM user WHERE login = '{account}'"
            response = cursor.execute(cmd)
            if not response:
                return {'success': 0, 'message': 'Такого аккаунта нет в базе данных'}
            shared_secret = cursor.fetchone()['shared_secret']
            code = generate_one_time_code(shared_secret)
            for _ in range(5):
                if not code:
                    code = generate_one_time_code(shared_secret)
                else:
                    return {'success': 1, 'result': code}
            if not code:
                return {'success': 0, 'message': 'Не удалось получить код Guard'}
    except:
        return {'success': 0, 'message': traceback.format_exc()}


def AddProxy(proxy: str, type: str, accounts: str = None, country: str = None, private: int = 0, working: int = 1, end_date: int =  time.time() + 2592000) -> dict:
    try:
        connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            proxy = 'http://' + proxy.strip().split(':')[2] + ':' + proxy.strip().split(':')[3] + '@' + proxy.strip().split(':')[
                0] + ':' + proxy.strip().split(':')[1]
        except:
            return {'success': 0, 'message': 'Неверный формат прокси. Шаблон: 127.0.0.1:5000:log:pass'}
        with connection.cursor() as cursor:
            cmd = f"SELECT * FROM proxy WHERE proxy = '{proxy}' AND type = '{type}'"
            cursor.execute(cmd)
            if cursor.fetchone():
                return {'success': 0, 'message': 'Такой прокси уже есть'}

            columns = []
            for column in ['proxy', 'type', 'private', 'country', 'accounts', 'working', 'end_date']:
                if eval(column):
                    columns.append(column)
            print('___proxy___', proxy)
            values = []
            for column in columns:
                values.append(str(eval(column)))
            columns_str = (', ').join(columns)
            values_str = "'" + ("', '").join(values) + "'"
            cmd = f"INSERT INTO `proxy` ({columns_str}) VALUES ({values_str})"

            cursor.execute(cmd)
        connection.commit()
        return {'success': 1, 'message': 'Прокси добавлен'}
    except:
        return {'success': 0, 'message': traceback.format_exc()}


def GetProxy(sel_acc: str = None, add_acc: str = None, type_: str = 'Steam', ) -> dict:
    try:
        connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            if sel_acc:
                cmd = f"SELECT * FROM proxy WHERE accounts LIKE '%{sel_acc}%' AND working = 1"
            elif type_:
                cmd = f"SELECT * FROM proxy WHERE type = '{type_}' AND working = 1 "
            cursor.execute(cmd)
            # Сортирует прокси по количеству аккаунтов (по количеству пробелов в accounts). Сначала идут те, у которых аккаунтов меньше всего
            proxy = sorted([pr for pr in cursor.fetchall()], key=lambda pr: pr['accounts'].count(' ') if type(pr['accounts']) == str else -1)
            if proxy:
                proxy = proxy.pop(0)
            if proxy and add_acc and not sel_acc:
                if type(proxy['accounts']) == str and proxy['accounts'].count(' ') > 1:
                    return {'success': 0, 'message': 'Нет свободных прокси'}
                if proxy['accounts']:
                    accounts_str = proxy['accounts'] + ' ' + add_acc
                else:
                    accounts_str = add_acc
                cmd = f"UPDATE `proxy` SET `accounts` = '{accounts_str}' WHERE (`proxy` = '{proxy['proxy']}')"
                cursor.execute(cmd)
        connection.commit()
        if proxy:
            return {'success': 1, 'result': proxy}
        else:
            return {'success': 0, 'message': 'Нет свободных прокси'}
    except:
        return {'success': 0, 'message': traceback.format_exc()}


def FullProxy(type_: str) -> dict:
    try:
        # Здесь я получаю из базы данных все прокси
        connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            cmd = f"SELECT * FROM proxy WHERE type = '{type_}'"
            cursor.execute(cmd)
            proxies = cursor.fetchall()
        if proxies:
            return {'success': 1, 'result': proxies}
        else:
            return {'success': 0, 'message': 'Таких прокси нет'}
    except:
        return {'success': 0, 'message': traceback.format_exc()}


def CheckProxy(proxy: str) -> dict:
    try:
        proxy = {'https': proxy}
        url = 'https://steamcommunity.com/market/'
        status_code = requests.get(url, proxies=proxy).status_code
        if status_code == 200:
            return {'success': 1, 'result': 1}
        else:
            return {'success': 0, 'result': 0}
    except:
        return {'success': 0, 'message': traceback.format_exc()}




def SteamCreateSSN(login_: str, time_: int = 0) -> steampy.client.SteamClient | dict:
    try:
        connection = pymysql.connect(host='127.0.0.1', user='root', password='password', database='database',
                                     cursorclass=pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            cmd = f"SELECT session FROM user WHERE login = '{login_}'"
            cursor.execute(cmd)
            data = cursor.fetchone()

            if not data:
                return {'success': 0, 'message': 'Такого аккаунта нет в базе данных'}
            else:
                session = data.get('session')

            proxy = GetProxy(sel_acc=login_)
            if not proxy.get('success'):
                proxy = GetProxy(add_acc=login_, type_='Steam')
            if not proxy.get('success'):
                return {'success': 0, 'message': 'Нет свободных прокси'}
            proxy = {'https': proxy['result']['proxy']}

            new_time = NewTime(time_)
            cmd = "UPDATE user SET time = %s WHERE login = %s"
            cursor.execute(cmd, (new_time, login_))
            connection.commit()

            steam_client = None
            if session:
                steam_client = pickle.loads(session)
            try:
                if (not steam_client) or (not steam_client.is_session_alive()):
                    cmd = f"SELECT login, password, steam_id, api_key, shared_secret, identity_secret FROM user WHERE login = '{login_}'"
                    cursor.execute(cmd)
                    acc_data = cursor.fetchone()


                    login_ = acc_data['login']
                    password = acc_data['password']
                    api_key = acc_data['api_key']
                    steam_id = acc_data['steam_id']
                    shared_secret = acc_data['shared_secret']
                    identity_secret = acc_data['identity_secret']
                    steam_json = {
                        "steamid": steam_id,
                        "shared_secret": shared_secret,
                        "identity_secret": identity_secret
                    }
                    try:
                        steam_client = SteamClient(api_key=api_key, proxies=proxy)
                    except steampy.exceptions.ProxyConnectionError:
                        return {'success': 0, 'message': 'Неправильные api_key или proxy'}
                    try:
                        print('Данные:', login_, password, steam_json)
                        steam_client.login(login_, password, steam_json)
                    except:
                        print(traceback.format_exc())
                        return {'success': 0, 'message': 'Неправильные login, password, steam_id, shared_secret или identity_secret'}
                    steam_client_bytes = pickle.dumps(steam_client)
                    cmd = "UPDATE user SET session = %s, time = %s WHERE login = %s"
                    cursor.execute(cmd, (steam_client_bytes, new_time, login_))
                    connection.commit()
                    if steam_client.is_session_alive():
                        return {'success': 1, 'result': {'login': login_, 'session': pickle.dumps(steam_client), 'proxy': proxy, 'time': new_time}}
                elif steam_client.is_session_alive():
                    return {'success': 1, 'result': {'login': login_, 'session': pickle.dumps(steam_client), 'proxy': proxy, 'time': new_time}}
            except:
                print(traceback.format_exc())
                return {'success': 0, 'message': traceback.format_exc()}
        return {'success': 0, 'message': 'Не удалось получить сессию'}
    except:
        print(traceback.format_exc())
        return {'success': 0, 'message': traceback.format_exc()}


def NewTime(minutes: int):
    url = "http://worldtimeapi.org/api/timezone/Europe/Moscow"
    response = requests.get(url)
    data = response.json()
    moscow_time = data.get('datetime')
    if moscow_time:
        current_time = moscow_time.replace('T', ' ').replace('+03:00', '')
    else:
        current_time = datetime.datetime.now()
    date_format = "%Y-%m-%d %H:%M:%S.%f"
    datetime_object = datetime.datetime.strptime(current_time, date_format)

    new_time = str(datetime_object + datetime.timedelta(minutes=minutes))
    return new_time


class Main(Resource):
    def get(self, encrypted_cmd):
        response = encrypted_cmd
        return {'response': response}
    def post(self, encrypted_cmd):
        data = pickle.loads(decrypt(key, request.data))
        if encrypted_cmd == 'AddAccount':
            account_data = data
            result = AddAccount(account_data=account_data)
        elif encrypted_cmd == 'GetProxy':
            login = data.get('login')
            type_ = data.get('type')
            result = GetProxy(sel_acc=login, type_=type_)
        elif encrypted_cmd == 'FullProxy':
            type_ = data['type']
            result = FullProxy(type_=type_)
        elif encrypted_cmd == 'RewriteAccount':
            account_data = data
            result = RewriteAccount(account_data=account_data)
        elif encrypted_cmd == 'DelAccount':
            account = data['login']
            result = DelAccount(account=account)
        elif encrypted_cmd == 'ConfirmSteamOne':
            login = data['login']
            item_id = data['id']
            result = ConfirmSteamOne(login_=login, item_id_=item_id)
        elif encrypted_cmd == 'ConfirmSteamAll':
            login = data['login']
            result = ConfirmSteamAll(login_=login)
        elif encrypted_cmd == 'SteamCreateSSN':
            login = data['login']
            time = data['time']
            result = SteamCreateSSN(login_=login, time_=time)
        elif encrypted_cmd == 'GenerateCode':
            login = data['login']
            result = GenerateCode(account=login)
        elif encrypted_cmd == 'SteamEndSSN':
            login = data['login']
            result = SteamEndSSN(login_=login)
        elif encrypted_cmd == 'SendSteamOne':
            login = data['login']
            id_ = data['id']
            game = data['game']
            url = data['url']
            result = SendSteamOne(login_=login, id_=id_, game=game, trade_offer_url=url)
        elif encrypted_cmd == 'SendSteamAll':
            login = data['login']
            game = data['game']
            url = data['url']
            result = SendSteamAll(login_=login, game=game, trade_offer_url=url)
        elif encrypted_cmd == 'AddProxy':
            proxy = data['proxy']
            type_ = data['type']
            country = data['country']
            working = data['working']
            private = data.get('private')
            accounts = data.get('accounts')
            end_date = data.get('end_date')
            result = AddProxy(proxy, type_, accounts, country, private, working, end_date)
            print(result)

        if type(result) != bytes:
            data_b = pickle.dumps(result)
        else:
            data_b = result

        enc_resp = encrypt(key, data_b)
        response = make_response(enc_resp)
        if result['success'] == 0:
            response.status_code = 400
        response.headers['Content-Type'] = 'application/octet-stream'
        return response


if __name__ == '__main__':
    app = Flask(__name__)
    api = Api()
    api.add_resource(Main, '/api/<string:encrypted_cmd>')
    api.init_app(app)
    app.run(debug=True, port=5000, host='185.224.135.50')
