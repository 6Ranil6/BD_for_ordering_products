from mimesis import Payment
from mimesis.locales import Locale
import psycopg2
import numpy as np
import random
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

class PushCard:
    payment = Payment()
    connect = psycopg2.connect(dbname = 'DeliveryProductS',\
                                   user = 'postgres',\
                                   password = 'bn554540',\
                                   host = 'localhost')

    def __init__(self):
        self._data = []
    
    @classmethod
    def close_transaction(cls):
        cls.connect.commit()
        cls.connect.close()


    def create_data_for_card(self):
        MAX_CARD_NUMBER = 4
        cursor = self.connect.cursor()
        cursor.execute("SELECT person_id FROM person")
        all_person_id = np.array(cursor.fetchall())[:,0].tolist()
        set_credit_card = set()
        card_id = 0
        for person_id in all_person_id:
            for i in range(random.randint(1, MAX_CARD_NUMBER)):
                
                credit_cart_number = Decimal("".join(self.payment.credit_card_number().split()))
                while credit_cart_number in set_credit_card:
                    print(credit_cart_number)
                    credit_cart_number = Decimal("".join(self.payment.credit_card_number().split()))
                set_credit_card.add(credit_cart_number)

                start_year = int((datetime.today().date() - timedelta(days= 30 * 12 * 2)).year)
                print(start_year)
                end_year = int((datetime.today().date() + timedelta(days= 30 * 12 * 8)).year)
                print(end_year)

                credit_cvc_number = self.payment.cvv() # одно и тоже с CVC, просто для mastercard CVC для VISA CVV
                card = {'card_id' : card_id,
                        'person_id': person_id,
                        'card_number': credit_cart_number,
                        'CVC': credit_cvc_number,
                        'end_date': date(year= random.randint(start_year, end_year), month= random.randint(1, 12), day= random.randint(1, 28))}
                card_id += 1
                self._data.append(card)


    def print_data_about_card(self, n = 5):
        
        for i in range(n):
            print("card_id =", self._data[i]["card_id"])
            print("person_id =", self._data[i]["person_id"])
            print("card_number =", self._data[i]['card_number'])
            print("CVC =", self._data[i]["CVC"])
            print("end_date =", self._data[i]['end_date'])
            print("---------------------")

    def push_data_about_card(self):
        cursor = self.connect.cursor()
        data_for_input = [(card['card_id'], card['person_id'], card['card_number'], card['CVC'], card['end_date']) for card in self._data]
        cursor.executemany("INSERT INTO card(card_id, person_id, card_number, cvc, end_date)\
                            VALUES\
                            (%s, %s, %s, %s, %s)", data_for_input)

def main():
    a = PushCard()
    a.create_data_for_card()
    a.print_data_about_card()
    a.push_data_about_card()
    a.close_transaction()
if __name__ == "__main__":
    main() 
