import scrapy
import json
import datetime
import re
import requests
import pandas as pd
from ..items import AlfaorgItem
from ..pipelines import AlfaorgPipeline
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
    name = "alfaorg"

    def start_requests(self):
        urls = []
        for item in nev:
            urls.append(nev[item])
        for url in urls:
            yield scrapy.Request(url)

    def parse(self, response):
        items = AlfaorgItem()

        lot_id = re.findall(
            r'\d+', response.xpath('//*[@id="ctl00_ctl00_contentHolder"]/fieldset[3]/legend/text()').get())[0]
        # Организатор торгов
        organization_trade_table = response.css(
            '#ctl00\$ctl00\$MainContent\$ContentPlaceHolderMiddle\$ppInfo\$ecProducer'
        ).get()
        organization_trade_table_data_frame = pd.read_html(organization_trade_table)

        inn_organaizer = organization_trade_table_data_frame[0].loc[0, 3]
        organizer = organization_trade_table_data_frame[0].loc[0, 1]

        # Контактное лицо организатора торгов
        contact_person_table = response.css(
            '#ctl00\$ctl00\$MainContent\$ContentPlaceHolderMiddle\$ctl02\$ContactPersonInfo'
        ).get()
        contact_person_table_data_frame = pd.read_html(contact_person_table)
        contact_person = contact_person_table_data_frame[0].loc[0, 1]
        phone = contact_person_table_data_frame[0].loc[1, 1]

        # Информация о публичном предложении
        # Поиск адреса электронной почты
        auction_info_table = response.css(
            '#ctl00\$ctl00\$MainContent\$ContentPlaceHolderMiddle\$PurchaseMainInfo\$PurchaseDetails'
        ).get()
        auction_info_table_data_frame = pd.read_html(auction_info_table)
        find_mail = auction_info_table_data_frame[0].loc[5, 1]
        # \S matches any non-whitespace character
        # @ for as in the Email
        # + for Repeats a character one or more times
        email = re.findall('\S+@\S+', find_mail)
        if len(email) == 0:
            email = 'Нет информации'

        # Информация о должнике
        bankrupt_table = response.css(
            '#ctl00\$ctl00\$MainContent\$ContentPlaceHolderMiddle\$ctl03\$BankruptDetailsInfo'
        ).get()
        bankrupt_table_data_frame = pd.read_html(bankrupt_table)
        if bankrupt_table_data_frame[0].loc[0, 1] == 'Физическое лицо':
            bankrupt = bankrupt_table_data_frame[0].loc[1, 1]
            bankrupt_href = f'https://fedresurs.ru/search/entity?code={bankrupt_table_data_frame[0].loc[2, 1]}'
            inn_bankruptcy = bankrupt_table_data_frame[0].loc[2, 1]
            fedresurs = f'https://fedresurs.ru/search/entity?code={bankrupt_table_data_frame[0].loc[2, 1]}'
        else:
            bankrupt = bankrupt_table_data_frame[0].loc[2, 1]
            bankrupt_href = f'https://fedresurs.ru/search/entity?code={bankrupt_table_data_frame[0].loc[1, 1]}'
            inn_bankruptcy = bankrupt_table_data_frame[0].loc[1, 1]
            fedresurs = f'https://fedresurs.ru/search/entity?code={bankrupt_table_data_frame[0].loc[1, 1]}'
        dt = datetime.datetime.now().isoformat(timespec='hours')

        items['lot_id'] = lot_id
        items['etp'] = ''
        items['lot_number'] = ''
        items['description_short'] = ''
        items['price_actual'] = ''
        items['lot_link'] = ''
        items['description'] = ''
        items['price_start'] = ''
        items['chart_price'] = ''
        items['price_market'] = ''
        items['price_step'] = ''
        items['deposit'] = ''
        items['cadastral_value'] = ''
        items['category'] = ''
        items['subcategory'] = ''
        items['address'] = ''
        items['auction_type'] = ''
        items['auction_status'] = ''
        items['application_start'] = ''
        items['application_deadline'] = ''
        items['bankrupt'] = str(bankrupt)
        items['bankrupt_href'] = str(bankrupt_href)
        items['inn_bankruptcy'] = str(inn_bankruptcy)
        items['contact_person'] = str(contact_person)
        items['phone'] = str(phone)
        items['email'] = str(email)
        items['inn_organizer'] = str(inn_organaizer)
        items['trading_number'] = ''
        items['lot_online'] = ''
        items['fedresurs'] = str(fedresurs)
        items['organizer'] = str(organizer)
        items['organizer_link'] = str('https://bankrupt.alfalot.ru')
        items['image_links'] = ''
        items['etp_latitude'] = ''
        items['etp_longitude'] = ''
        items['kadastr_price'] = ''
        items['market_price'] = ''
        items['square_zem_value'] = ''
        items['square_value'] = ''
        items['flat_rooms'] = ''
        items['latitude'] = ''
        items['longitude'] = ''
        items['image_links_external'] = ''
        items['update_time'] = dt

        yield items
