# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector


class AlfalotPipeline:

    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        # Установление соединения с БД
        self.conn = mysql.connector.connect(
            host='localhost',  # адрес БД
            user='root',  # Имя пользователя
            passwd='4vvt3pd3',  # Пароль
            database='alfalot'  # Имя БД
        )
        self.curr = self.conn.cursor()

    def create_table(self):
        self.curr.execute("""DROP TABLE IF EXISTS alfa_lot_tb""")
        self.curr.execute("""CREATE TABLE alfa_lot_tb (
        lot_id text,
        etp text,
        lot_number text,
        description_short text,
        price_actual text,
        lot_link text,
        description text,
        price_start text,
        chart_price text,
        price_market text,
        price_step text,
        deposit text,
        cadastral_value text,
        category text,
        subcategory text,
        address text,
        auction_type text,
        auction_status text,
        application_start text,
        application_deadline text,
        bankrupt text,
        bankrupt_href text,
        inn_bankruptcy text,
        contact_person text,
        phone text,
        email text,
        inn_organizer text,
        trading_number text,
        lot_online text,
        fedresurs text,
        organizer text,
        organizer_link text,
        image_links text,
        etp_latitude text,
        etp_longitude text,
        kadastr_price text,
        market_price text,
        square_value text,
        square_zem_value text,
        flat_rooms text,
        latitude text,
        longitude text,
        image_links_external text,
        update_time text
        );""")

    def process_item(self, item, spider):
        self.store_db(item)
        self.conn.commit()  # Для корректной работы на удаленном сервере
        return item

    def store_db(self, item):
        self.curr.execute("""
        INSERT INTO alfa_lot_tb VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s)""", (
            item['lot_id'],
            item['etp'],
            item['lot_number'],
            item['description_short'],
            item['price_actual'],
            item['lot_link'],
            item['description'],
            item['price_start'],
            item['chart_price'],
            item['price_market'],
            item['price_step'],
            item['deposit'],
            item['cadastral_value'],
            item['category'],
            item['subcategory'],
            item['address'],
            item['auction_type'],
            item['auction_status'],
            item['application_start'],
            item['application_deadline'],
            item['bankrupt'],
            item['bankrupt_href'],
            item['inn_bankruptcy'],
            item['contact_person'],
            item['phone'],
            item['email'],
            item['inn_organizer'],
            item['trading_number'],
            item['lot_online'],
            item['fedresurs'],
            item['organizer'],
            item['organizer_link'],
            item['image_links'],
            item['etp_latitude'],
            item['etp_longitude'],
            item['kadastr_price'],
            item['market_price'],
            item['square_value'],
            item['square_zem_value'],
            item['flat_rooms'],
            item['latitude'],
            item['longitude'],
            item['image_links_external'],
            item['update_time']
        )
                          )
