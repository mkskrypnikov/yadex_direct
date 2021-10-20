import requests
import mytoken
from requests.exceptions import ConnectionError
from time import sleep
import json
import datetime
from datetime import datetime as dt
from datetime import date, timedelta
import time
import sys
import pandas as pd
import os



def get_date(LastDate):
    my_date = []
    for i in range(LastDate):
        temp_date = datetime.datetime.now()
        temp_date = temp_date - timedelta(days=i+1)
        my_date.append(temp_date.strftime("%Y-%m-%d"))
    return my_date


def rep(token,login,date_from,date_to,yad_metricks,type_reports):
    if sys.version_info < (3,):
        def u(x):
            try:
                return x.encode("utf8")
            except UnicodeDecodeError:
                return x
    else:
        def u(x):
            if type(x) == type(b''):
                return x.decode('utf8')
            else:
                return x

    # --- Входные данные ---
    # Адрес сервиса Reports для отправки JSON-запросов (регистрозависимый)
    ReportsURL = 'https://api.direct.yandex.com/json/v5/reports'

    # OAuth-токен пользователя, от имени которого будут выполняться запросы
    token = token

    # Логин клиента рекламного агентства
    # Обязательный параметр, если запросы выполняются от имени рекламного агентства
    clientLogin = login

    # --- Подготовка запроса ---
    # Создание HTTP-заголовков запроса
    headers = {
        # OAuth-токен. Использование слова Bearer обязательно
        "Authorization": "Bearer " + token,
        # Логин клиента рекламного агентства
        "Client-Login": clientLogin,
        # Язык ответных сообщений
        "Accept-Language": "ru",
        # Режим формирования отчета
        "processingMode": "auto"
        # Формат денежных значений в отчете
        # "returnMoneyInMicros": "false",
        # Не выводить в отчете строку с названием отчета и диапазоном дат
        # "skipReportHeader": "true",
        # Не выводить в отчете строку с названиями полей
        # "skipColumnHeader": "true",
        # Не выводить в отчете строку с количеством строк статистики
        # "skipReportSummary": "true"
    }
    datename = str(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))


    # Создание тела запроса
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": yad_metricks,
            "ReportName": u(datename),
            "ReportType": type_reports,
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    # Кодирование тела запроса в JSON
    body = json.dumps(body, indent=4)

    # --- Запуск цикла для выполнения запросов ---
    # Если получен HTTP-код 200, то выводится содержание отчета
    # Если получен HTTP-код 201 или 202, выполняются повторные запросы
    while True:
        try:
            req = requests.post(ReportsURL, body, headers=headers)
            req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
            if req.status_code == 400:
                print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(u(body)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            elif req.status_code == 200:

                format(u(req.text))
                break
            elif req.status_code == 201:
                print("Отчет успешно поставлен в очередь в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 202:
                print("Отчет формируется в режиме офлайн")
                retryIn = int(req.headers.get("retryIn", 60))
                print("Повторная отправка запроса через {} секунд".format(retryIn))
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                sleep(retryIn)
            elif req.status_code == 500:
                print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            elif req.status_code == 502:
                print("Время формирования отчета превысило серверное ограничение.")
                print(
                    "Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                print("JSON-код запроса: {}".format(body))
                print("RequestId: {}".format(req.headers.get("RequestId", False)))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break
            else:
                print("Произошла непредвиденная ошибка")
                print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                print("JSON-код запроса: {}".format(body))
                print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            print("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            break

        # Если возникла какая-либо другая ошибка
        except:
            # В данном случае мы рекомендуем проанализировать действия приложения
            print("Произошла непредвиденная ошибка")
            # Принудительный выход из цикла
            break

    json_string = json.dumps(body)
    return req.text

def replacement_data(dict_date):
    df_replace = pd.read_csv('yad.csv', sep=';', encoding='cp1251')
    for dates in dict_date:
        df_replace = df_replace[df_replace.Date.str.contains(dates) == False]
    df_replace.to_csv("yad.csv", index=False, header=True, sep=";", encoding='cp1251')



def yad_mcc(token,project,DateFrom,DateTo,yad_metricks,type_reports,LastDate,replacement):
    if LastDate > 0:
        dict_date = get_date(LastDate)
        DateFrom = dict_date[-1]
        DateTo = dict_date[0]
    itog = pd.DataFrame()
    print('выгрузка данных:')
    print(DateFrom)
    print(DateTo)
    for i in project:
        data = rep(token, i, DateFrom, DateTo, yad_metricks, type_reports)
        file = open("cashe.csv", "w")
        file.write(data)
        file.close()
        f = pd.read_csv('cashe.csv', sep='	', encoding='cp1251', header=1)
        f['account'] = i
        itog = itog.append(f, ignore_index=False)
        time.sleep(1)
    itog['Cost'] = itog['Cost'] / 1000000
    itog = itog[itog.Date.str.contains('Total') == False]
    if replacement == 'no':
        itog.to_csv("yad.csv", index=False, header=True, sep=";", encoding='cp1251')
    else:
        replacement_data(dict_date)
        x1 = pd.read_csv('yad.csv', sep=';', encoding='cp1251', header=0)
        x1 = itog.append(x1, ignore_index=False)
        x1.to_csv('yad.csv', index=False, header=True, sep=';', encoding='cp1251')

    path = os.getcwd()
    f = path+'\cashe.csv'
    if os.path.isfile(f):
        os.remove(f)

