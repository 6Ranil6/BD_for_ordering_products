import psycopg2
import random
from datetime import datetime, timedelta
from mimesis import Address
from mimesis.locales import Locale

class PushCustomerOrder:
    connect = psycopg2.connect(dbname='DeliveryProductS',
                             user='postgres',
                             password='bn554540',
                             host='localhost')
    address_gen = Address(locale=Locale.RU)

    def __init__(self):
        self._orders = []
        self._client_ids = []
        self._basket_ids = []
        self._store_ids = []
        self._status_ids = []

    def get_ids_from_db(self):
        cursor = self.connect.cursor()
        
        print("Получаем client_id из client...")
        cursor.execute("SELECT client_id FROM client")
        self._client_ids = [row[0] for row in cursor.fetchall()]
        
        print("Получаем basket_id из basket...")
        cursor.execute("SELECT basket_id FROM basket")
        self._basket_ids = [row[0] for row in cursor.fetchall()]
        
        print("Получаем store_id из store...")
        cursor.execute("SELECT store_id FROM store")
        self._store_ids = [row[0] for row in cursor.fetchall()]
        
        print("Получаем status_id из order_status...")
        cursor.execute("SELECT status_id FROM order_status")
        self._status_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.close()

    def create_data(self, n=200000):
        self.get_ids_from_db()
        
        if not all([self._client_ids, self._basket_ids, self._store_ids, self._status_ids]):
            print("Ошибка: Одна из связанных таблиц пуста!")
            print(f"Найдено client_ids: {len(self._client_ids)}")
            print(f"Найдено basket_ids: {len(self._basket_ids)}")
            print(f"Найдено store_ids: {len(self._store_ids)}")
            print(f"Найдено status_ids: {len(self._status_ids)}")
            return False
        
        print("Создание данных для заказов...")
        start_date = datetime.now() - timedelta(days=365*2)  # Заказы за последние 2 года
        
        for i in range(n):
            # Случайная дата в диапазоне 2 лет
            random_date = start_date + timedelta(seconds=random.randint(0, 365*2*24*60*60))
            
            order = {
                'order_id': i,
                'client_id': random.choice(self._client_ids),
                'basket_id': random.choice(self._basket_ids),
                'store_id': random.choice(self._store_ids),
                'status_id': random.choice(self._status_ids),
                'creation_date': random_date,
                'address': self.address_gen.address(),
                'cost': round(random.uniform(100, 50000), 2),
                'total_weight': round(random.uniform(0.1, 50.0), 2)
            }
            self._orders.append(order)
            
            if i % 10000 == 0 and i != 0:
                print(f"Создано {i} записей")
        
        return True

    def push_to_db(self):
        """Вставляем данные в БД"""
        if not self._orders:
            print("Нет данных для вставки!")
            return
        
        cursor = self.connect.cursor()
        print("Начало вставки данных в БД...")
        
        try:
            for i, order in enumerate(self._orders):
                cursor.execute(
                    "INSERT INTO customer_order(order_id, client_id, basket_id, store_id, status_id, "
                    "creation_date, address, total_cost, total_weight) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (order['order_id'], order['client_id'], order['basket_id'], 
                     order['store_id'], order['status_id'], order['creation_date'],
                     order['address'], order['cost'], order['total_weight'])
                )
                
                if i % 1000 == 0 and i != 0:
                    self.connect.commit()
                    print(f"Добавлено {i} записей")
            
            self.connect.commit()
            print("Все данные успешно добавлены!")
            
        except Exception as e:
            print(f"Ошибка при вставке: {e}")
            self.connect.rollback()
        finally:
            cursor.close()

    @classmethod
    def close_connection(cls):
        cls.connect.close()

def main():
    pusher = PushCustomerOrder()
    
    if pusher.create_data(n=1_000_000):
        pusher.push_to_db()
    
    pusher.close_connection()

if __name__ == "__main__":
    main()
