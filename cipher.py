import numpy as np
def Encrypt(message) -> str:
    if type(message) != str:
        message = str(message)
    key1 = [4, 17, 6, 9, 7, 8, 3, 11, 15, 14, 5, 2, 1, 13, 19, 18, 0, 16, 10, 12]
    key2 = [9, 7, 19, 13, 11, 14, 1, 16, 3, 18, 2, 17, 12, 8, 5, 6, 0, 10, 15, 4]
    c_num = round(len(message) ** 0.5 + 0.4999)
    s_num = round(len(message) / c_num + 0.4999)
    key1_sl = key1[:c_num]
    key2_sl = key2[:s_num]
    def norm(key_sl):
        sorted_key_sl = sorted(key_sl)
        return [sorted_key_sl.index(num) for num in key_sl]
    normed_c_num = norm(key1_sl)
    normed_s_num = norm(key2_sl)
    message = message + (s_num * c_num - len(message)) * ' '
    lst_message = list(message)
    mtx = [[lst_message.pop(0) for i in range(c_num)] for j in range(s_num)]
    np_mtx = np.matrix(mtx)

    #Encrypt
    np_mtx[sorted(normed_s_num), :] = np_mtx[normed_s_num, :]
    np_mtx[:, sorted(normed_c_num)] = np_mtx[:, normed_c_num]
    return ''.join(np_mtx.flatten().tolist()[0])

def Decrypt(message) -> str:
    if type(message) != str:
        message = str(message)
    key1 = [4, 17, 6, 9, 7, 8, 3, 11, 15, 14, 5, 2, 1, 13, 19, 18, 0, 16, 10, 12]
    key2 = [9, 7, 19, 13, 11, 14, 1, 16, 3, 18, 2, 17, 12, 8, 5, 6, 0, 10, 15, 4]
    c_num = round(len(message) ** 0.5 + 0.4999)
    s_num = round(len(message) / c_num + 0.4999)
    key1_sl = key1[:c_num]
    key2_sl = key2[:s_num]
    def norm(key_sl):
        sorted_key_sl = sorted(key_sl)
        return [sorted_key_sl.index(num) for num in key_sl]
    normed_c_num = norm(key1_sl)
    normed_s_num = norm(key2_sl)
    message = message + (s_num * c_num - len(message)) * ' '
    lst_message = list(message)
    mtx = [[lst_message.pop(0) for i in range(c_num)] for j in range(s_num)]
    np_mtx = np.matrix(mtx)

    #Decrypt
    np_mtx[normed_s_num, :] = np_mtx[sorted(normed_s_num), :]
    np_mtx[:, normed_c_num] = np_mtx[:, sorted(normed_c_num)]
    return ''.join(np_mtx.flatten().tolist()[0]).strip()