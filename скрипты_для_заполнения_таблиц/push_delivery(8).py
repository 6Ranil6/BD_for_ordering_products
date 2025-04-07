import psycopg2
import random
from datetime import datetime

class PushDelivery:
    connect = psycopg2.connect(dbname='DeliveryProductS',
                             user='postgres',
                             password='bn554540',
                             host='localhost')

    def __init__(self):
        self._deliveries = []
        self._order_ids = []
        self._courier_ids = []
        self._status_ids = []

    def get_ids_from_db(self):
        """Получаем ID из связанных таблиц"""
        cursor = self.connect.cursor()
        
        # Получаем order_id из customer_order
        cursor.execute("SELECT order_id FROM customer_order")
        self._order_ids = [row[0] for row in cursor.fetchall()]
        
        # Получаем courier_id из courier
        cursor.execute("SELECT courier_id FROM courier")
        self._courier_ids = [row[0] for row in cursor.fetchall()]
        
        # Получаем status_id из order_status
        cursor.execute("SELECT status_id FROM order_status")
        self._status_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.close()

    def create_data(self, n=200000):
        """Создаем данные для доставки"""
        self.get_ids_from_db()
        
        if not self._order_ids or not self._courier_ids or not self._status_ids:
            print("Ошибка: Одна из связанных таблиц пуста!")
            print(f"Найдено order_ids: {len(self._order_ids)}")
            print(f"Найдено courier_ids: {len(self._courier_ids)}")
            print(f"Найдено status_ids: {len(self._status_ids)}")
            return False
        
        print("Создание данных для доставки...")
        for i in range(n):
            delivery = {
                'delivery_id': i,
                'order_id': random.choice(self._order_ids),
                'courier_id': random.choice(self._courier_ids),
                'status_id': random.choice(self._status_ids),
                'payment_status': random.choice([True, False])
            }
            self._deliveries.append(delivery)
            
            # Выводим прогресс каждые 10к записей
            if i % 10000 == 0 and i != 0:
                print(f"Создано {i} записей")
        
        return True

    def push_to_db(self):
        """Вставляем данные в БД"""
        if not self._deliveries:
            print("Нет данных для вставки!")
            return
        
        cursor = self.connect.cursor()
        print("Начало вставки данных в БД...")
        
        try:
            for i, delivery in enumerate(self._deliveries):
                cursor.execute(
                    "INSERT INTO delivery(delivery_id, order_id, courier_id, status_id, payment_status) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (delivery['delivery_id'], delivery['order_id'], delivery['courier_id'], 
                     delivery['status_id'], delivery['payment_status'])
                )
                
                # Коммитим каждые 1000 записей
                if i % 1000 == 0 and i != 0:
                    self.connect.commit()
                    print(f"Добавлено {i} записей")
            
            # Финальный коммит
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
    pusher = PushDelivery()
    
    if pusher.create_data(n=200000):
        pusher.push_to_db()
    
    pusher.close_connection()

if __name__ == "__main__":
    main()
