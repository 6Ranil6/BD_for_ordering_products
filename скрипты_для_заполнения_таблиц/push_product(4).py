import psycopg2
from mimesis import Food, Person
from mimesis.locales import Locale
import random

class PushProduct:
    connect = psycopg2.connect(dbname='DeliveryProductS',
                              user='postgres',
                              password='bn554540',
                              host='localhost')
    food = Food(locale=Locale.RU)
    person = Person(locale=Locale.RU)

    def __init__(self):
        self._data = []

    def create_data_for_product(self, n=100000):
        for i in range(n):
            info_product = {
                'product_id': i,
                'store_id': random.randint(0, 999),
                'name': self.food.dish(),
                'weight': round(random.uniform(0.1, 10.0), 2),
                'stock_quantity': random.randint(1, 1000),
                'cost': round(random.uniform(50, 5000), 2) 
            }
            self._data.append(info_product)

    def print_data_about_product(self, n=5):
        for product in self._data[:n]:
            print("product_id =", product["product_id"])
            print("store_id =", product['store_id'])
            print("name =", product['name'])
            print("weight =", product['weight'])
            print("stock_quantity =", product['stock_quantity'])
            print("cost =", product['cost'])
            print("------------------")

    def push_product(self):
        cursor = self.connect.cursor()
        data_for_input = [
            (product['product_id'], product['store_id'], product['name'], 
             product['weight'], product['stock_quantity'], product['cost'])  # Добавлен cost
            for product in self._data]
        cursor.executemany(
            "INSERT INTO product(product_id, store_id, name, weight, stock_quantity, cost) "
            "VALUES (%s, %s, %s, %s, %s, %s)",  # Добавлен cost
            data_for_input
        )
    @classmethod
    def commit_and_close_connect(cls):
        cls.connect.commit()
        cls.connect.close()

def main():
    a = PushProduct()
    a.create_data_for_product()
    a.print_data_about_product()
    a.push_product()
    a.commit_and_close_connect()

if __name__ == "__main__":
    main()
