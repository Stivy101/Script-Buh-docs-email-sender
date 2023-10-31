import psycopg2
from pathlib import *
import pandas as pd
from sqlalchemy import create_engine, text
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

"""В самом начале пробросим необходимые соединения, у нас таких два:
    1. С SQL сервером, на котором собирается информация о проведённых бухгалтерских документах
    2.C почтовым сервером, который будет от нашего имени рассылать письма"""

user = input('Введите имя пользователя БД')
password = input('Введите пароль для пользователя БД')
host = input('Введите имя хоста, где находится ваша БД')
port = input('Введите адрес порта вашего БД сервера')
bd_name = input('Введите название базы данных для подключения')
try:
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{bd_name}')
    conn = engine.connect()
    print('Подключение к серверу SQL успешно создано')
except:
    print('Подключение к SQL серверу не удалось, проверьте данные для подключения')


"""Для рассылки писем я решил использовать SMTP-сервер от Mail.ru, для авторизации нужно создать
приложение в их экосистеме и получить к нему пароль (который по сути является токеном доступа)"""
try:
    login = input('Введите адрес электронной почты, от которой будут отправляться письма')
    password = input('Введите пароль, полученный от владельца SMTP-сервера')
    server = smtplib.SMTP(host='smtp.mail.ru', port=587, local_hostname='comp4')
    server.starttls()
    server.login(login, password)
    print("Подключение к SMTP-серверу успешно создано")
except:
    print('Подключение к SMTP-серверу не удалось, проверьте данные для подключения')

"""Выгрузим информацию о документах из нашей базы данных, сджойнив таблицу документов с таблицей контрагентов,
чтобы получить их электронные адреса.
Прим.: в базе названия фирм-партнёров хранятся в кавычках. Уберём кавычки, чтоб они не помешали"""

req_text = "SELECT docs.index, Дата, Вид, Сумма, Статус, partners.Контрагент, partners.Почта"
                      "FROM docs INNER JOIN partners ON docs.Контрагент = partners.Контрагент"
                      "WHERE Статус = 'Не получен'"
df = pd.read_sql(text(req_text), conn, index_col = 'index')
df['Контрагент'] = df['Контрагент'].apply(lambda x: x.replace('\"',''))

""" Следующим блоком кода мы получаем список всех "нерадивых поставщиков", собираем для каждого список недостающих
документов и формируем письмо с этим списком, которое отправляется через SMTP-сервер"""

def form_msg(bd_rows):
    bd_rows = [list(i) for i in bd_rows]
    msg = """Уважаемый партнёр! Компания 'Лучшая компания' сообщает, что по итогам проверки обнаружилось отсутствие
    некоторых закрывающих документов с вашей подписью. Высылаем реквизиты этих документов в списке ниже и просим
    поспособствовать их скорейшей доставке в нашу бухгалтерию по адресу: 123007, г. Москва, Хорошевское шоссе,
    д.5, оф. 231\nСписок документов:"""
    spisok = ''
    for i in bd_rows:
        spisok = spisok + f'\n{i[1]} от {i[0]} на сумму {i[2]} руб'
    full_msg = msg + '\n' + spisok
    return full_msg

ka_list = df['Контрагент'].unique()
for i in ka_list:
    net_zakr = df.query(f'Контрагент  == "{i}"')
    adress = df[df['Контрагент'] == i]['Почта'][0:1].values[0]
    pismo = form_msg(net_zakr.values.tolist())
    mes = MIMEMultipart()
    mes['FROM'] = 'bibop.2010@mail.ru'
    mes['TO'] = 'bibop.2015@mail.ru'
    mes['Subject'] = 'Просим выслать экземпляры документов'
    mes.attach(MIMEText(pismo, 'plain'))
    server.sendmail(mes['From'], mes['TO'], mes.as_string())

"""Уходя, гасим свет и закрываем коннекции с SQL и SMTP серверами"""
conn.close()
server.quit()