import os
import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import creds
import requests
from xml.etree import ElementTree
import psycopg2

# Получение данных из таблицы с вынесением пароля в отдельный файл
def get_service_simple():
    return build('sheets', 'v4', developerKey=creds.api_key)

def get_service_sacc():
    creds_json = os.path.dirname(__file__) + "/creds/sacc1.json"
    scopes = ['https://www.googleapis.com/auth/spreadsheets']

    creds_service = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scopes).authorize(httplib2.Http())
    return build('sheets', 'v4', http=creds_service)

service = get_service_sacc()
sheet = service.spreadsheets()

sheet_id = "1caxDIQvs168YH9OFsSsNOy8Jrx-maAHUNyvpwdH1QHE"

resp = sheet.values().get(spreadsheetId=sheet_id, range="Лист1!A1:D").execute()

resp['values'].pop(0)

# Парсинг XML с сайта банка

response = requests.get('https://www.cbr.ru/scripts/XML_daily.asp')
tree = ElementTree.fromstring(response.content)
curs = float(tree[10][4].text.replace(',', '.'))

# Подключение к БД

con = psycopg2.connect(
    database="TEST",
    user="postgres",
    password="test",
    host="127.0.0.1",
    port="3211"
)

cur = con.cursor()

# Получаем все ID с базы данных, что бы избежать повторений

cur.execute('SELECT "заказ №" AS "id" FROM TEST')
idArray = []
for row in cur.fetchall():
    idArray.append(row[0])

# Переменные для SQL
insertSQL = ''
updateSQL = ''
deleteSQL = ''

# Фильтр пустых значений
tableXML = list(filter(lambda x: len(list(x)) > 3 and list(x[0]) and list(x[1]) and list(x[2]), resp['values']))

# Изменение базы данных
for item in tableXML:
    priceRR = round(int(item[2]) * curs, 0)
    if int(item[1]) in idArray:
        updateSQL += 'UPDATE TEST SET "№" = ' + item[0] + ', "стоимость,$" = ' + item[2] + ', "срок поставки" = \'' + item[3] + '\', "стоимость,RR" = ' + str(priceRR) + ' WHERE "заказ №" = ' + item[1] + ';'
        idArray.remove(int(item[1]))
    else:
        insertSQL += "('" + "', '".join(item) + "', '" + str(priceRR) + "'), "
else:
    cur.execute(updateSQL)
    if insertSQL != '':
        cur.execute('INSERT INTO TEST VALUES ' + insertSQL[:-2] + ';')
    if idArray:
        for delSQL in idArray:
            deleteSQL += 'DELETE FROM TEST WHERE "заказ №" = ' + str(delSQL)
        else:
            cur.execute(deleteSQL)


con.commit()
con.close()
