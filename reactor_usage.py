import datetime
import pickle
import time

import requests

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

with open('key.bin', 'rb') as file:
    key = file.read()


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


class Reactor:
    def __init__(self, host_port: str):
        self.host_port = host_port

    def get_response(self, cmd: str, data: dict):
        url = f"http://{self.host_port}/api/{cmd}"
        headers = {'Content-Type': 'application/octet-stream'}
        dumped_data = pickle.dumps(data)
        encrypted_data = encrypt(key, dumped_data)
        rs = requests.post(url=url, data=encrypted_data, headers=headers)
        decrypted_data = pickle.loads(decrypt(key, rs.content))
        return decrypted_data

    def add_account(self,
                    login: str,
                    password: str,
                    shared_secret: str,
                    steam_id: str,
                    identity_secret: str,
                    api_key: str):
        data = {
            "login": login,
            "password": password,
            "shared_secret": shared_secret,
            "steam_id": steam_id,
            "identity_secret": identity_secret,
            "api_key": api_key
        }
        cmd = "AddAccount"
        return self.get_response(cmd, data)

    def delete_account(self, login):
        data = {
            "login": login
        }
        cmd = "DelAccount"
        return self.get_response(cmd, data)

    def rewrite_account(self,
                        login: str,
                        password: str,
                        shared_secret: str,
                        steam_id: str,
                        identity_secret: str,
                        api_key: str):
        data = {
            "login": login,
            "password": password,
            "shared_secret": shared_secret,
            "steam_id": steam_id,
            "identity_secret": identity_secret,
            "api_key": api_key
        }
        cmd = "RewriteAccount"
        return self.get_response(cmd, data)

    def add_proxy(self,
                  proxy: str,
                  proxy_type: str = 'Steam',
                  country: str = 'RU',
                  max_usages: int = 4,
                  working: int = 1,
                  private: int = 1,
                  end_date = datetime.datetime.now() + datetime.timedelta(days=30)):
        data = {
            "proxy": proxy,
            "type": proxy_type,
            "max_usages": max_usages,
            "country": country,
            "working": working,
            "private": private,
            "end_date": end_date
        }
        cmd = "AddProxy"
        return self.get_response(cmd, data)

    def create_ssn(self,
                   login: str,
                   time: int = 86400):
        data = {
            "login": login,
            "time": time
        }
        cmd = "SteamCreateSSN"
        return self.get_response(cmd, data)

    def get_proxy(self,
                  proxy_type: str = 'Steam',
                  login: str = None):
        data = {
            "type": proxy_type,
            "login": login
        }
        cmd = "GetProxy"
        return self.get_response(cmd, data)

    def full_proxy(self,
                   proxy_type: str):
        data = {
            "type": proxy_type
        }
        cmd = "FullProxy"
        return self.get_response(cmd, data)

    def generate_code(self,
                      login: str):
        data = {
            "login": login
        }
        cmd = "GenerateCode"
        return self.get_response(cmd, data)

    def send_steam_all(self,
                       login: str,
                       game: str,
                       url: str):
        data = {
            "login": login,
            "game": game,
            "url": url
        }
        cmd = "SendSteamAll"
        return self.get_response(cmd, data)

# dct = { "login": "dani1ldolgov", "password": "SxGaSZOQ", "shared_secret": "kReFElcRGIMtLcSyGQ+VwveJ0iM=",
# "steamid": "76561199081494335", "identity_secret": "qqoDnsdzeIX0v/kU1HkJnJM2x4g=", "web_api":
# "D3D00BF26E986C73B046D5AECC405129" } 'dani1ldolgov', 'SxGaSZOQ', 'kReFElcRGIMtLcSyGQ+VwveJ0iM=',
# '76561199081494335', 'qqoDnsdzeIX0v/kU1HkJnJM2x4g=', 'D3D00BF26E986C73B046D5AECC405129'
reactor = Reactor('185.224.135.50:5000')
message = reactor.full_proxy('Steam')
print(message)