from mimesis import Person 
from mimesis.locales import Locale
from mimesis.builtins import RussiaSpecProvider
import random 
import numpy as np
import psycopg2 
from datetime import datetime

class PushPersonClientCourier:
    _person = Person(Locale.RU)
    _ru_spec_data = RussiaSpecProvider()
    connect = psycopg2.connect(dbname = 'DeliveryProductS',\
                                   user = 'postgres',\
                                   password = 'bn554540',\
                                   host = 'localhost')

    def __init__(self):
        self._data = []

    def create_data_for_person(self, n: int):
        #создаем данные
        for i in range(n):
            print(i)
            data_per_person = {
                'person_id' : i,
                'passport_series' : random.randint(1000, 9999),
                'passport_number' : int("".join(str(self._ru_spec_data.passport_number()).split())),
                'date_of_birth': self._person.birthdate(min_year= 1920, max_year= 2006),
                'name' : self._person.full_name(),
                'contact_info' : self._person.telephone()
            }
            self._data.append(data_per_person)

    def create_data_for_courier(self):
        number_all_data = len(self._data)
        self._unique_id_courier = random.sample(range(0, number_all_data), k= int(0.1 * number_all_data))
    
    def create_data_for_client(self):
        number_all_data = len(self._data)
        all_id = np.arange(start= 0,\
                           stop= number_all_data,\
                           step= 1)
        mask = ~np.isin(all_id, self._unique_id_courier)
        random_couriers = np.array(random.sample(self._unique_id_courier, k= int(len(self._unique_id_courier) * 0.2)))
        self._unique_id_client = np.concatenate((all_id[mask], random_couriers), axis= None).tolist()

    def print_data_for_person(self, n = 5):
        """_summary_
            Выводит первые  n строк
        Args:
            n (int, optional): количество строк. Defaults to 10.
        """
        for i in range(n):
            data_per_person = self._data[i]
            for key in data_per_person.keys():
                print(f"{key} = {data_per_person[key]}")
            print(f"\n ---------")
        print(f'number person = {len(self._data)}')

    def print_data_for_client(self, n = 5):
        """
        Вывод данных о клиенте
        Args:
            n (int, optional): количество строк . Defaults to 5.
        """
        for i in range(n):
            print(f'person_id_for_client {i} = {self._unique_id_client[i]}')
        print(f'number client = {len(self._unique_id_client)}')
        print("_________________")

    def print_data_for_courier(self, n = 5):
        """
        Вывод данных о курьере
        Args:
            n (int, optional): количество строк . Defaults to 5.
        """
        for i in range(n):
            print(f'person_id_for_client {i} = {self._unique_id_courier[i]}')
        print(f'number courier = {len(self._unique_id_courier)}')
        print("_________________")
    
    def push_person(self):
        cursor = self.connect.cursor()
        for item in self._data:
            # Подготавливаем данные
            person_id = item['person_id']
            passport_series = str(item['passport_series'])
            passport_number = str(item['passport_number'])
            date_of_birth = datetime.strptime(str(item['date_of_birth']), '%Y-%m-%d').date()
            name = item['name']
            contact_info = item['contact_info']

            cursor.execute(
                "INSERT INTO person(person_id, passport_series, passport_number, date_of_birth, name, contact_info) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (person_id, passport_series, passport_number, date_of_birth, name, contact_info)
            )
            
    def push_courier(self):
        cursor = self.connect.cursor()
        counter = 0
        for i in self._unique_id_courier:
            cursor.execute(f"INSERT INTO courier(courier_id, person_id) VALUES({counter}, {i});")
            counter += 1
    
    def push_client(self):
        cursor = self.connect.cursor()
        counter = 0
        for i in self._unique_id_client:
            cursor.execute(f"INSERT INTO client(client_id, person_id) VALUES({counter}, {i});")
            counter += 1

    @classmethod
    def commit_ADD_DATA(cls):
        cls.connect.commit()

    @classmethod
    def close_db(cls):
        cls.connect.close()

def main():
    pp = PushPersonClientCourier()

    pp.create_data_for_person(n = 1000) 
    pp.create_data_for_courier()
    pp.create_data_for_client()

    pp.push_person()
    pp.commit_ADD_DATA()
    pp.push_courier()
    pp.commit_ADD_DATA()
    pp.push_client()
    pp.commit_ADD_DATA()
    pp.close_db()
    # pp.print_data_for_person()
    # pp.print_data_for_courier()
    # pp.print_data_for_client()

if __name__ == "__main__":
    main()
