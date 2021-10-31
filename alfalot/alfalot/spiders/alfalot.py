import scrapy
import json
import datetime
import re
import requests
import pandas as pd
from ..items import AlfalotItem
from ..pipelines import AlfalotPipeline
from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


def grab_links_for_start():
    # Начальный адрес
    url = 'https://bankrupt.alfalot.ru/public/purchases-all/'
    # Подключаем браузер и загружаем страницу
    driver = webdriver.Chrome()

    driver.get(url)

    # Устанавливаем статус на 'Идет прием заявок'
    status = driver.find_element(By.ID,
                                 "ctl00_ctl00_MainExpandableArea_phExpandCollapse_PurchasesSearchCriteria_vPurchaseLot_purchaseStatusID_Статус")
    status.send_keys(Keys.ARROW_DOWN, Keys.ARROW_DOWN, Keys.ENTER)
    find_trades = driver.find_element(By.ID, "ctl00_ctl00_MainExpandableArea_phExpandCollapse_SearchButton")
    find_trades.send_keys(Keys.ENTER)
    time.sleep(3)  # Ожидание загрузки данных

    # Словарь ссылок для начала сбора данных основным пауком
    links = {}

    # счетчик для поиска ссылок на страницу
    count = 1
    pages = [1]

    def page():
        lot_links = []
        org_links = []
        # Берем ссылки со страницы

        lot_links_selector = driver.find_elements(
            By.CSS_SELECTOR,
            "#ctl00_ctl00_MainContent_ContentPlaceHolderMiddle_PurchasesSearchResult > tbody td:nth-child(4) [href] "
        )  # Ссылки на страницу лота
        lot_links_list = [elem.get_attribute('href') for elem in lot_links_selector]

        org_links_selector = driver.find_elements(
            By.CSS_SELECTOR,
            "#ctl00_ctl00_MainContent_ContentPlaceHolderMiddle_PurchasesSearchResult > tbody td:nth-child(1) [href] "
        )  # Ссылки на страницу организатора
        org_links_list = [elem.get_attribute('href') for elem in org_links_selector]

        # Готовим чистые ссылки
        for a in lot_links_list:
            if 'http' in a:
                lot_links.append(a)
        for a in org_links_list:
            if 'http' in a:
                org_links.append(a)

        # Создаем словарь для передачи данных в другую функцию
        return dict(zip(lot_links, org_links))

    # Получаем ссылки с первой страницы и запускаем цикл для сбора с остальных страниц
    links.update(page())
    while count <= 11:
        # XPATH для перехода по страницам
        all_pages = f'//*[@id="ctl00_ctl00_MainContent_ContentPlaceHolderMiddle_PurchasesSearchResult"]/tbody/tr[' \
                    f'1]/td/a[{count}] '
        p = driver.find_element(By.XPATH, all_pages)
        # Условие для невозврата к начальным страницам
        if p.text == "<<":
            count += 1
        # Переход на следующие 10 страниц
        elif p.text == ">>":
            count = 1
            p.click()
            time.sleep(3)  # Ожидание загрузки данных
            links.update(page())  # Собираем ссылки
            # 10-я ссылка в списке только на первой странице содержит ">>" в остальном будет цифрой
            e = driver.find_element(By.XPATH,
                                    '//*[@id="ctl00_ctl00_MainContent_ContentPlaceHolderMiddle_PurchasesSearchResult"]/tbody/tr[1]/td/a[10]').text
            # Кроме 1-й и последней страницы длинна будет равна 11
            le = driver.find_elements(By.XPATH,
                                      '//*[@id="ctl00_ctl00_MainContent_ContentPlaceHolderMiddle_PurchasesSearchResult"]/tbody/tr[1]/td/a')
            # В условии проверяем что ушли с первой страницы и дошли до последней
            if int(e) > 11 and len(le) == 10:
                # Последний номер на последней странице
                all_pages = f'//*[@id="ctl00_ctl00_MainContent_ContentPlaceHolderMiddle_PurchasesSearchResult"]/tbody' \
                            f'/tr[1]/td/a[10] '
                lp = driver.find_element(By.XPATH, all_pages)
                # Номер последней страницы с предпоследней страницы
                lpp = pages[-1]
                # Узнаем сколько осталось собрать страниц
                digit = int(lp.text) - int(lpp)
                # В цикле проходим от последней страницы до собраных ранее страниц
                for count in range(10, (10 - digit), -1):
                    all_pages = f'//*[@id="ctl00_ctl00_MainContent_ContentPlaceHolderMiddle_PurchasesSearchResult' \
                                f'"]/tbody/tr[1]/td/a[{count}] '
                    p = driver.find_element(By.XPATH, all_pages)
                    p.click()
                    pages.append(p.text)
                    time.sleep(3)  # Ожидание загрузки данных
                    links.update(page())  # Собираем ссылки
                count = 15  # Устанавливаем условие для завершения цикла
        else:
            # Собираем ссылки
            p.click()
            pages.append(p.text)
            # Проверка чтобы не было дублей (возможно не нужна)
            if p.text in pages:
                pass
            count += 1
            time.sleep(3.2)  # Ожидание загрузки данных
            links.update(page())  # Собираем ссылки

    driver.close()  # Закрываем браузер
    return links

def osm_data(data_from_rosreestr):
    # Перемпнные для составления адреса запроса
    osm_house, osm_street = data_from_rosreestr['osm_house'], data_from_rosreestr['osm_street']
    osm_town = data_from_rosreestr['osm_place']
    # Первый альтернативный адрес для поска коордиат
    osm_alt_address = data_from_rosreestr['osm_alt']
    url = ''
    # osm_coordinates = f"https://nominatim.openstreetmap.org/search?q={lat},{lon}&format=json"
    alt_osm_address = f"https://nominatim.openstreetmap.org/search?q={osm_alt_address}&format=json"
    main_osm_address = f"https://nominatim.openstreetmap.org/search?q={osm_house}+{osm_street}+{osm_town}&format=json"
    # osm_lot_address = f"https://nominatim.openstreetmap.org/search?q={address}&format=json"
    if osm_street == '' or None:
        url = alt_osm_address
    else:
        url = main_osm_address
    try:
        a = requests.get(url)
        json_osm = json.loads(a.text)
        return json_osm[0]
    except:
        json_osm_err = {
            'lat': "Not found",
            'lon': "Not found",
            'display_name': "Not found"
        }
        return json_osm_err


def room_finder(description):
    room_in_digit = {
        "Однокомнатная": 1,
        "1-а ком": 1,
        "1-ком": 1,
        "1 ком": 1,
        "1 - ком": 1,
        "1- ком": 1,
        "Двухкомнатная": 2,
        "2-х ком": 2,
        "2-ком": 2,
        "2 ком": 2,
        "2 - ком": 2,
        "2- ком": 2,
        "Трехкомнатная": 3,
        "Трёхкомнатная": 3,
        "3-а ком": 3,
        "3-ком": 3,
        "3 ком": 3,
        "3 - ком": 3,
        "3- ком": 3,
        "Четырехкомнатная": 4,
        "Четырёхкомнатная": 4,
        "4-х ком": 4,
        "4-ком": 4,
        "4 ком": 4,
        "4 - ком": 4,
        "4- ком": 4,
        "Свободная планировка": 0,
        "Пятикомнатная": 5,
        "5-и ком": 5,
        "5-ком": 5,
        "5 ком": 5,
        "5 - ком": 5,
        "5- ком": 5,
    }
    patterns = [
        "Однокомнатная", "Двухкомнатная", "Трехкомнатная", "Трёхкомнатная", "Четырехкомнатная",
        "Четырёхкомнатная", "Пятикомнатная", "Свободная планировка",
        "\d+-а ком", "\d+-ком", "\d+ ком", "\d+ - ком", "\d+- ком"
    ]

    for pat in patterns:
        a = re.findall(pat, description)
        for i in a:
            if i in room_in_digit.keys():
                return (room_in_digit[i])
            else:
                try:
                    digit = int(re.findall(r'\d+', i)[0])
                    if digit > 5:
                        return (digit)
                    else:
                        return (room_in_digit[i])
                except:
                    continue


def find_cadastral_value(text):
    # Итоговый список номеров
    numbers = []
    # Поиск кадастровых номеров в тексте полного описания
    values = list(re.findall(r'([0-9]+):([0-9]+):([0-9]+):([0-9]+)', text))
    # Собирает полученые данные в правильном формате
    if len(values) >= 1:
        for idx, val in enumerate(values):
            numbers.append(":".join(values[idx]))
    else:
        # Передается для создания url адреса в функции получения данных из росреестра
        numbers.append("11:11:11:11")
    return numbers


def rosreestr(numbers):
    cad_data = []
    cad_data1 = []
    house, street, place, merge = '', '', '', ''
    for item in numbers:
        a, b, c, d = int(item.split(":")[0]), int(item.split(":")[1]), int(item.split(":")[2]), int(item.split(":")[3])
        url = (f"https://rosreestr.gov.ru/api/online/fir_object/{a}:{b}:{c}:{d}")
        body = requests.get(url)
        if body.status_code != 200:
            cad_data.append("Not connect")
            cad_data1.append("Not connect")
            continue
        else:
            try:
                cadastr_json = json.loads(body.text)
                cad_data.append(cadastr_json["parcelData"]['cadCost'])
                house = cadastr_json['objectData']['objectAddress']['house']
                street = cadastr_json['objectData']['objectAddress']['street'] + " " + cadastr_json['objectData'][
                    'objectAddress']['streetType'] + "."
                if cadastr_json["parcelData"]['areaValue'] == '' or None:
                    cad_data1.append(cadastr_json["parcelData"]['areaUnitValue'])
                else:
                    cad_data1.append(cadastr_json["parcelData"]['areaValue'])
                if cadastr_json['objectData']['objectAddress']['place'] is None:
                    if cadastr_json['objectData']['objectAddress']['region'] == 78:
                        place = "Санкт-Петербург"
                    elif cadastr_json['objectData']['objectAddress']['region'] == 77:
                        place = "Москва"
                    else:
                        place = cadastr_json['objectData']['objectAddress']['locality']
                else:
                    place = cadastr_json['objectData']['objectAddress']['place']
                if street is None:
                    merge = cadastr_json['objectData']['addressNote']
                    if merge is None:
                        merge = cadastr_json['objectData']['objectAddress']['addressNotes']
                else:
                    merge = cadastr_json['objectData']['objectAddress']['mergedAddress']
                    if merge is None:
                        merge = cadastr_json['objectData']['objectAddress']['addressNotes']
            except:
                merge = cadastr_json['objectData']['addressNote']
                continue
    osm_dict = {"osm_house": house, "osm_street": street, "osm_place": place, "osm_alt": merge}
    return cad_data, cad_data1, osm_dict


nev = grab_links_for_start()


class AlfaSpider(scrapy.Spider):
    name = "alfalot"

    def start_requests(self):
        urls = []
        for item in nev:
            urls.append(item)
        for url in urls:
            yield scrapy.Request(url)

    def parse(self, response):
        items = AlfalotItem()
        # Страница с информацией о лоте разбитан на таблицы
        # Переменные для таблиц
        # Информация об аукционе - из этой таблицы берется "Номер сообщения в ЕФРСБ - trading_number"
        # и "Форма торга по составу участников - auction_type(тип торгов)"
        auction_inf_table_html = response.css(
            '#ctl00\$ctl00\$MainContent\$ContentPlaceHolderMiddle\$ctl01\$PurchaseInfoViewContainer').get()
        auction_inf_table_data_frame = pd.read_html(auction_inf_table_html)

        # Информация о лоте - из этой таблицы берутся короткое и полное описание, даты торгов,
        # цены, шаг цены и категория (согласно классификатору ЕФРСБ)
        lot_inf_table_html = response.css(
            '#ctl00\$ctl00\$MainContent\$ContentPlaceHolderMiddle\$LotInfo\$PurchaseLotViewContainer'
        ).get()
        lot_inf_table_data_frame = pd.read_html(lot_inf_table_html)

        # Обеспечение заявки - из этой таблицы берется депозит
        tender_security_html = response.css(
            '#ctl00\$ctl00\$MainContent\$ContentPlaceHolderMiddle\$ctl06\$BackingInfoViewContainer'
        ).get()
        tender_security_data_frame = pd.read_html(tender_security_html)

        # Идентификатор лота (уникальный)
        lot_id = str(
            re.findall(
                r'\d+', response.xpath('//*[@id="ctl00_ctl00_contentHolder"]/fieldset[1]/legend/text()').get())[0]
        )

        # Площадка(ЭТП)
        etp = "Электронная торговая площадка Alfalot.ru"

        # Номер лота
        lot_number = response.xpath(
            '//*[@id="ctl00_ctl00_contentHolder"]/fieldset[2]/legend/text()').get().replace(
            '\r\n\t\t\tИнформация о лоте ', '').replace('\r\n\t\t', '')

        # Краткое Описание
        description_short = lot_inf_table_data_frame[0].loc[1, 1]

        # Текущая цена
        if 'Текущая цена' in lot_inf_table_data_frame[0].loc[3, 3]:
            price_actual = lot_inf_table_data_frame[0].loc[2, 3].replace('\xa0', '')
        else:
            price_actual = lot_inf_table_data_frame[0].loc[3, 3].replace('\xa0', '')

        # Ссылка на лот
        lot_link = response.url

        # Описание
        description = ""
        # Шаг цены
        price_step = ""
        # Категория (Квартира/Аппартамент/Дом/Зем.участок и т.п.)
        category = ""
        # Начало подачи заявок
        application_start = ""
        # Окончание подачи заявок
        application_deadline = ""
        for i in range(1, len(lot_inf_table_data_frame[0][2])):
            if 'Сведения об иму' in lot_inf_table_data_frame[0].loc[i, 2]:
                description = lot_inf_table_data_frame[0].loc[i, 3]
            if 'Шаг, руб.:' in lot_inf_table_data_frame[0].loc[i, 2]:
                price_step = lot_inf_table_data_frame[0].loc[i, 3].replace('\xa0', '')
            if 'Классификатор ЕФРСБ:' in lot_inf_table_data_frame[0].loc[i, 2]:
                category = lot_inf_table_data_frame[0].loc[i, 3]
        for i in range(1, len(lot_inf_table_data_frame[0][2])):
            if 'начала' in lot_inf_table_data_frame[0].loc[i, 2]:
                application_start = lot_inf_table_data_frame[0].loc[i, 3]
            if 'Дата окончания' in lot_inf_table_data_frame[0].loc[i, 2]:
                application_deadline = lot_inf_table_data_frame[0].loc[i, 3]

        # Начальная цена
        price_start = lot_inf_table_data_frame[0].loc[3, 3].replace('\xa0', '')

        # График цены
        chart_price = {}
        try:
            chart_price_table = response.css(
                "#ctl00_ctl00_MainContent_ContentPlaceHolderMiddle_publicOfferReduction_srPublicOfferReductionPeriod").get()
            price_data_frame = pd.read_html(chart_price_table)
            k, v = [], []
            for i in range(1, len(price_data_frame[0][3])):
                k.append(price_data_frame[0].loc[i, 3])
                v.append(price_data_frame[0].loc[i, 6].replace('\xa0', ''))
            chart_price = dict(zip(k, v))
        except:
            chart_price = "Нет графика цены"

        # Рыночная цена
        price_market = "N/A"

        # Размер аванса
        deposit = tender_security_data_frame[0].loc[1, 1].replace('\xa0', '')

        # Кадастровый номер
        cadastral_value = find_cadastral_value(description)

        # Подкатегория (при наличии)
        subcategory = "N/A"

        # Адрес
        address = "N/A"

        # Тип торгов
        auction_type = f'{auction_inf_table_data_frame[0].loc[0, 0]} {auction_inf_table_data_frame[0].loc[0, 1]}'

        # Статус торгов
        auction_status = "Идёт прием заявок"

        # Наименование банкрота
        bankrupt = 'On page two'

        # Ссылка на инфо о банкротстве
        bankrupt_href = f'https://fedresurs.ru/search/entity?code= + On page two'

        # ИНН банкрота
        inn_bankruptcy = 'On page two'

        # Контактное лицо
        contact_person = "On pge two"

        # Телефон
        phone = "On page two"

        # e-mail
        email = "On page two"

        # ИНН организатора
        inn_organizer = ""

        # Номер торгов
        trading_number = ""
        for i in range(0, len(auction_inf_table_data_frame[0][2])):
            if 'Номер сообщения в ЕФРСБ:' in auction_inf_table_data_frame[0].loc[i, 2]:
                trading_number = auction_inf_table_data_frame[0].loc[i, 3]

        # Ссылка на торговую площадку
        lot_online = 'https://bankrupt.alfalot.ru/'

        # Ссылка на fedresurs
        fedresurs = "https: // fedresurs.ru / search / entity?code  + INN On page two"

        # Организатор
        organizer = "On page two"

        # Ссылка на организатора
        organizer_link = "N/A"

        # сылки на фото объекта
        image_links = "N/A"

        # (С площадки) Географические координаты (широта)
        etp_latitude = "N/A"

        #  (С площадки) Географические координаты (долгота)
        etp_longitude = "N/A"

        # Кадастровая стоимость
        # Переменная содержащая список с ответами от api росреестра
        reestr = rosreestr(cadastral_value)
        kadastr_price = reestr[0]

        # Координаты с внешнего сайта OpenStreets.com
        osm_coordinates = osm_data(reestr[2])

        # Рыночная цена
        market_price = "N/A"

        # Площадь построек/квартиры
        square_value = reestr[1]

        # Площадь участка
        square_zem_value = reestr[1]

        # Кол-во комнат (для квартир)
        flat_rooms = "N/A"

        #  Географические координаты (широта)
        latitude = "N/A"

        # Географические координаты (долгота)
        longitude = "N/A"

        # Ссылки на фото объекта
        image_links_external = "N/A"

        # Поиск количства комнат в квартире
        flat_rooms = room_finder(description)

        # Временная метка для БД
        dt = datetime.datetime.now().isoformat(timespec='hours')

        yield {
            'lot_id': str(lot_id),
            'etp': str(etp),
            'lot_number': str(lot_number),
            'description_short': str(description_short),
            'price_actual': str(price_actual),
            'lot_link': str(lot_link),
            'description': str(description),
            'price_start': str(price_start),
            'chart_price': str(chart_price),
            'price_market': str(price_market),
            'price_step': str(price_step),
            'deposit': str(deposit),
            'cadastral_value': str(cadastral_value),
            'category': str(category),
            'subcategory': str(subcategory),
            'address': str(address),
            'auction_type': str(auction_type),
            'auction_status': str(auction_status),
            'application_start': str(application_start),
            'application_deadline': str(application_deadline),
            'bankrupt': str(bankrupt),
            'bankrupt_href': str(bankrupt_href),
            'inn_bankruptcy': str(inn_bankruptcy),
            'contact_person': str(contact_person),
            'phone': str(phone),
            'email': str(email),
            'inn_organizer': str(inn_organizer),
            'trading_number': str(trading_number),
            'lot_online': str(lot_online),
            'fedresurs': str(fedresurs),
            'organizer': str(organizer),
            'organizer_link': str(organizer_link),
            'image_links': str(image_links),
            'etp_latitude': str(etp_latitude),
            'etp_longitude': str(etp_longitude),
            'kadastr_price': str(kadastr_price),
            'market_price': str(market_price),
            'square_zem_value': str(square_zem_value),
            'square_value': str(square_value),
            'flat_rooms': str(flat_rooms),
            'latitude': str(osm_coordinates['lat']),
            'longitude': str(osm_coordinates['lon']),
            'image_links_external': str(image_links_external),
            'update_time': str(dt),
        }
