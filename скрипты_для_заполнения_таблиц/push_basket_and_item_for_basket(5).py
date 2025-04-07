import psycopg2
from datetime import datetime, timedelta
import random

class PushBasket:
    connect = psycopg2.connect(dbname='DeliveryProductS',
                              user='postgres',
                              password='bn554540',
                              host='localhost')

    def __init__(self):
        self._baskets = []
        self._basket_items = []

    def create_data_for_baskets(self, n=500):
        for i in range(n):
            random_date = datetime.now() - timedelta(days=random.randint(0, 365))
            
            basket = {
                'basket_id': i,
                'date_added': random_date.date()
            }
            self._baskets.append(basket)

    def create_data_for_basket_items(self, n=2000):
        cursor = self.connect.cursor()
        cursor.execute("SELECT product_id FROM product")
        product_ids = [row[0] for row in cursor.fetchall()]

        for i in range(n):
            item = {
                'item_id': i,
                'basket_id': random.choice([b['basket_id'] for b in self._baskets]),
                'product_id': random.choice(product_ids),
                'quantity': random.randint(1, 10)
            }
            self._basket_items.append(item)

    def print_sample_data(self, n=5):
        print("Корзины:")
        for basket in self._baskets[:n]:
            print(f"basket_id: {basket['basket_id']}, date_added: {basket['date_added']}")
        
        print("\nЭлементы корзины:")
        for item in self._basket_items[:n]:
            print(f"item_id: {item['item_id']}, basket_id: {item['basket_id']}, "
                  f"product_id: {item['product_id']}, quantity: {item['quantity']}")

    def push_data_to_db(self):
        cursor = self.connect.cursor()
        
        baskets_data = [(b['basket_id'], b['date_added']) for b in self._baskets]
        cursor.executemany(
            "INSERT INTO basket(basket_id, date_added) VALUES (%s, %s)",
            baskets_data
        )
        
        items_data = [
            (i['item_id'], i['basket_id'], i['product_id'], i['quantity'])
            for i in self._basket_items
        ]
        cursor.executemany(
            "INSERT INTO item(item_id, basket_id, product_id, quantity) VALUES (%s, %s, %s, %s)",
            items_data
        )

    @classmethod
    def commit_and_close_connect(cls):
        cls.connect.commit()
        cls.connect.close()

def main():
    pusher = PushBasket()
    pusher.create_data_for_baskets()
    pusher.create_data_for_basket_items()
    pusher.print_sample_data()
    pusher.push_data_to_db()
    pusher.commit_and_close_connect()

if __name__ == "__main__":
    main()
