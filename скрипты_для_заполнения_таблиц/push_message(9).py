import psycopg2
import random
from datetime import datetime, timedelta
from mimesis import Text
from mimesis.locales import Locale

class PushMessages:
    connect = psycopg2.connect(dbname='DeliveryProductS',
                             user='postgres',
                             password='bn554540',
                             host='localhost')
    text_gen = Text(locale=Locale.RU)

    def __init__(self):
        self._messages = []
        self._client_ids = []
        self._order_ids = []
        self._courier_ids = []
        self._store_ids = []
        self._message_types = [
            'order_created', 'order_confirmed', 'payment_received',
            'order_assembled', 'courier_assigned', 'on_the_way',
            'arrived', 'delivered', 'delivery_problem', 'cancelled'
        ]

    def get_ids_from_db(self):
        """Получаем ID из связанных таблиц"""
        cursor = self.connect.cursor()
        
        print("Получаем client_id из client...")
        cursor.execute("SELECT client_id FROM client")
        self._client_ids = [row[0] for row in cursor.fetchall()]
        
        print("Получаем order_id из customer_order...")
        cursor.execute("SELECT order_id FROM customer_order")
        self._order_ids = [row[0] for row in cursor.fetchall()]
        
        print("Получаем courier_id из courier...")
        cursor.execute("SELECT courier_id FROM courier")
        self._courier_ids = [row[0] for row in cursor.fetchall()]
        
        print("Получаем store_id из store...")
        cursor.execute("SELECT store_id FROM store")
        self._store_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.close()

    def generate_content(self, msg_type):
        """Генерируем осмысленный текст сообщения"""
        templates = {
            'order_created': [
                "Ваш заказ №{order_id} успешно создан",
                "Мы получили ваш заказ №{order_id}",
                "Заказ №{order_id} оформлен"
            ],
            'order_confirmed': [
                "Заказ №{order_id} подтвержден",
                "Подтверждаем прием вашего заказа №{order_id}",
                "Ваш заказ №{order_id} принят в обработку"
            ],
            'payment_received': [
                "Оплата за заказ №{order_id} получена",
                "Мы получили оплату по заказу №{order_id}",
                "Заказ №{order_id} оплачен"
            ],
            'order_assembled': [
                "Заказ №{order_id} собран и готов к отправке",
                "Ваш заказ №{order_id} упакован",
                "Товары по заказу №{order_id} подготовлены"
            ],
            'courier_assigned': [
                "Курьер назначен для доставки заказа №{order_id}",
                "Ваш заказ №{order_id} передан курьеру",
                "Курьер получил заказ №{order_id}"
            ],
            'on_the_way': [
                "Курьер с заказом №{order_id} в пути",
                "Заказ №{order_id} доставляется",
                "Ваш заказ №{order_id} уже едет к вам"
            ],
            'arrived': [
                "Курьер с заказом №{order_id} прибыл",
                "Заказ №{order_id} у вашего дома",
                "Курьер ожидает вас с заказом №{order_id}"
            ],
            'delivered': [
                "Заказ №{order_id} успешно доставлен",
                "Вы получили заказ №{order_id}",
                "Доставка заказа №{order_id} завершена"
            ],
            'delivery_problem': [
                "Проблема с доставкой заказа №{order_id}",
                "Возникли сложности с доставкой заказа №{order_id}",
                "Задержка доставки заказа №{order_id}"
            ],
            'cancelled': [
                "Заказ №{order_id} отменен",
                "Ваш заказ №{order_id} был отменен",
                "Отмена заказа №{order_id}"
            ]
        }
        
        template = random.choice(templates[msg_type])
        order_id = random.choice(self._order_ids)
        return template.format(order_id=order_id)

    def create_data(self, n=1000000):
        """Создаем данные для сообщений"""
        self.get_ids_from_db()
        
        if not all([self._client_ids, self._order_ids, self._courier_ids, self._store_ids]):
            print("Ошибка: Одна из связанных таблиц пуста!")
            print(f"Найдено client_ids: {len(self._client_ids)}")
            print(f"Найдено order_ids: {len(self._order_ids)}")
            print(f"Найдено courier_ids: {len(self._courier_ids)}")
            print(f"Найдено store_ids: {len(self._store_ids)}")
            return False
        
        print("Создание данных для сообщений...")
        
        for i in range(n):
            msg_type = random.choice(self._message_types)
            
            message = {
                'message_id': i,
                'client_id': random.choice(self._client_ids),
                'order_id': random.choice(self._order_ids),
                'courier_id': random.choice(self._courier_ids),
                'store_id': random.choice(self._store_ids),
                'type': msg_type,
                'content': self.generate_content(msg_type)
            }
            self._messages.append(message)
            
            if i % 100000 == 0 and i != 0:
                print(f"Создано {i} записей")
        
        return True

    def push_to_db(self):
        """Вставляем данные в БД"""
        if not self._messages:
            print("Нет данных для вставки!")
            return
        
        cursor = self.connect.cursor()
        print("Начало вставки данных в БД...")
        
        try:
            for i, message in enumerate(self._messages):
                cursor.execute(
                    "INSERT INTO message(message_id, client_id, order_id, courier_id, store_id, type, content) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (message['message_id'], message['client_id'], message['order_id'],
                     message['courier_id'], message['store_id'], message['type'], message['content'])
                )
                
                if i % 10000 == 0 and i != 0:
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
    pusher = PushMessages()
    
    if pusher.create_data(n=1000000):
        pusher.push_to_db()
    
    pusher.close_connection()

if __name__ == "__main__":
    main()
