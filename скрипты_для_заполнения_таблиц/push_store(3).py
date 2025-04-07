import psycopg2
from mimesis import Address
from mimesis.locales import  Locale
import numpy as np
import random

class PushStore:
    connect = psycopg2.connect(dbname = 'DeliveryProductS',\
                               user = 'postgres',\
                               password = 'bn554540',\
                               host = 'localhost')
    faker = Address(locale= Locale.RU)

    def __init__(self):
        self._data = []

    def create_data_for_store(self, n = 1000):
        all_city  = ["г.Казань", 
                     "г.Москва", 
                     "г.Петербург"]
        
        name_store = ["Пятёрочка",
                      "Магнит",
                      "Лента",
                      "Ашан",
                      "Перекрёсток",
                      "О'Кей",
                      "Metro Cash & Carry",
                      "Дикси",
                      "ВкусВилл",
                      "Утконос",
                      "Ярче!",
                      "Красное & Белое",
                      "Азбука Вкуса",
                      "Глобус Гурмэ",
                      "Бахетле",
                      "СберМаркет",
                      "Утконос Онлайн",
                      "Ozon Fresh",
                      "Мираторг",
                      "ЛавкаЛавка"]
        
        for i in range(n):
            info_store = {'store_id': i,
                          'address' : self.faker.address() + ", " + random.choice(all_city),
                          'name' : random.choice(name_store)}            
            self._data.append(info_store)

    def print_data_about_store(self, n = 5):

        for store in np.array(self._data)[:n]:
            print("store_id =", store["store_id"])
            print("address =", store['address'])
            print("name =", store['name'])
            print("------------------")

    def push_city(self):
        cursor = self.connect.cursor()
        data_for_input = [ (store['store_id'], store['address'], store['name']) for store in self._data]
        cursor.executemany("INSERT INTO store(store_id, address, name)\
                           VALUES\
                           (%s, %s, %s)", data_for_input)
    @classmethod
    def commit_and_close_conect(cls):
        cls.connect.commit()
        cls.connect.close()

def main():
    a = PushStore()
    a.create_data_for_store()
    a.print_data_about_store()

    a.push_city()
    a.commit_and_close_conect()

if __name__ == "__main__":
    main()

