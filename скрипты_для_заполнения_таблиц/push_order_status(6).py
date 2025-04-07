import psycopg2

class PushOrderStatus:
    connect = psycopg2.connect(dbname='DeliveryProductS',
                              user='postgres',
                              password='bn554540',
                              host='localhost')

    def __init__(self):
        self._statuses = [
            {'status_id': 1, 'name': 'Создан'},
            {'status_id': 2, 'name': 'Ожидает оплаты'},
            {'status_id': 3, 'name': 'Оплачен'},
            {'status_id': 4, 'name': 'Проверка платежа'},
            {'status_id': 5, 'name': 'В обработке'},
            {'status_id': 6, 'name': 'Собран'},
            {'status_id': 7, 'name': 'Передан в доставку'},
            {'status_id': 8, 'name': 'В пути'},
            {'status_id': 9, 'name': 'На пункте выдачи'},
            {'status_id': 10, 'name': 'Доставлен'},
            {'status_id': 11, 'name': 'Отменен'},
            {'status_id': 12, 'name': 'Возврат'},
            {'status_id': 13, 'name': 'Частично возвращен'},
            {'status_id': 14, 'name': 'Ожидает подтверждения отмены'}]

    def print_statuses(self):
        print("Статусы заказов:")
        for status in self._statuses:
            print(f"status_id: {status['status_id']}, name: {status['name']}")

    def push_statuses_to_db(self):
        cursor = self.connect.cursor()
        cursor.executemany(
            "INSERT INTO order_status(status_id, name) VALUES (%s, %s)",
            [(s['status_id'], s['name']) for s in self._statuses]
        )

    @classmethod
    def commit_and_close_connect(cls):
        cls.connect.commit()
        cls.connect.close()

def main():
    pusher = PushOrderStatus()
    pusher.print_statuses()
    pusher.push_statuses_to_db()
    pusher.commit_and_close_connect()
    print("Статусы заказов успешно добавлены в БД")

if __name__ == "__main__":
    main()
