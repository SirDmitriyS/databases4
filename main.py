from typing import Iterable

import psycopg2


class Clients:
    
    def __init__(self, db, user, password, host) -> None:
        self.conn_props = {
            'database': db,
            'user': user,
            'password': password,
            'host': host
        }
        self.conn = psycopg2.connect(**self.conn_props)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()
    

    # Функция, создающая структуру БД (таблицы).
    def create_db(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS client (
                id SERIAL PRIMARY KEY,
                first_name varchar(100),
                last_name varchar(100),
                email varchar(100),
                constraint email_regexp check (email ~ '^[\w\-\.]+\@[\w\-\.]+\.[\w]+$')
            );
        """)

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS client_phone (
                id SERIAL PRIMARY KEY,
                client_id integer references client(id),
                phone varchar(40),
                constraint phone_format check (phone ~ '^[+]{0,1}\d{0,1}[(]{0,1}\d{1,4}[)]{0,1}[-\s\./\d]*$')
            );
        """)

        self.conn.commit()

    # Функция, позволяющая добавить нового клиента
    def add_client(self, first_name, last_name, email=None, phones=None):
        try:
            self.cur.execute("""
                    INSERT INTO client (first_name, last_name, email)
                    VALUES (%s, %s, %s)
                    RETURNING id;
                """,
                (first_name, last_name, email)
            )
            client_id = self.cur.fetchone()[0]

            if phones is not None and isinstance(phones, Iterable):
                for phone in phones:
                    self.add_phone(client_id, phone)

            self.conn.commit()

        except psycopg2.errors.CheckViolation:
            print('Возникла ошибка при добавлении клиента. Проверьте введеныне данные и повторите попытку.')
            self.conn.rollback()
            return

        return client_id

    # Функция, позволяющая добавить телефон для существующего клиента
    def add_phone(self, client_id, phone, do_commit=True):
        try:
            self.cur.execute("""
                    INSERT INTO client_phone (client_id, phone)
                    VALUES (%s, %s);
                """,
                (client_id, phone)
            )
            if do_commit:
                self.conn.commit()
        except psycopg2.errors.CheckViolation:
            print('Возникла ошибка при добавлении телефона. Проверьте введеныне данные и повторите попытку.')
            self.conn.rollback()

    # Функция, позволяющая изменить данные о клиенте
    def change_client(self, client_id, first_name=None, last_name=None, email=None, phones=None):
        updates = {}
        if first_name is not None:
            self.cur.execute("""
                    UPDATE client
                    SET
                        first_name = %s
                    WHERE id = %s;
                """,
                (first_name, client_id)
            )
        if last_name is not None:
            self.cur.execute("""
                    UPDATE client
                    SET
                        last_name = %s
                    WHERE id = %s;
                """,
                (last_name, client_id)
            )
        if email is not None:
            self.cur.execute("""
                    UPDATE client
                    SET
                        email = %s
                    WHERE id = %s;
                """,
                (email, client_id)
            )
        if phones is not None:
            self.cur.execute("""
                    DELETE FROM client_phone
                    WHERE client_id = %s;
                """,
                (client_id,)
            )
            for phone in phones:
                self.add_phone(client_id, phone)

            self.conn.commit()

    # Функция, позволяющая удалить телефон для существующего клиента
    def delete_phone(self, client_id, phone):
        self.cur.execute("""
                DELETE FROM client_phone
                WHERE
                    client_id = %s
                    AND phone = %s;
            """,
            (client_id, phone)
        )
        self.conn.commit()
    

    # Функция, позволяющая удалить существующего клиента
    def delete_client(self, client_id):
        self.cur.execute("""
                DELETE FROM client_phone
                WHERE client_id = %s;
            """,
            (client_id,)
        )
        self.cur.execute("""
                DELETE FROM client
                WHERE id = %s;
            """,
            (client_id,)
        )
        self.conn.commit()

    # Функция, позволяющая найти клиента по его данным: имени, фамилии, email или телефону
    def find_client(self, first_name=None, last_name=None, email=None, phone=None):
            self.cur.execute("""
                    SELECT DISTINCT c.id, c.first_name, c.last_name, c.email, string_agg(cp.phone, ', ') phones
                    FROM client c LEFT JOIN client_phone cp ON c.id = cp.client_id 
                    WHERE
                        (first_name = %(first_name)s or %(first_name)s IS NULL)
                        AND (last_name = %(last_name)s or %(last_name)s IS NULL)
                        AND (email = %(email)s or %(email)s IS NULL)
                        AND (phone = %(phone)s or %(phone)s IS NULL)
                    GROUP BY c.id, c.first_name, c.last_name, c.email;
                """,
                {
                    'first_name': first_name,
                    'last_name': last_name,
                    'email': email,
                    'phone': phone
                }
            )
            return self.cur.fetchall()

    # Вывод сведений о всех клиентах и их телефонах
    def print_all_clients(self):
        self.cur.execute("""
            SELECT c.id, c.first_name, c.last_name, c.email, string_agg(cp.phone, ', ') phones
            FROM client c LEFT JOIN client_phone cp ON c.id = cp.client_id
            GROUP BY c.id, c.first_name, c.last_name, c.email;
        """)
        for client in self.cur:
            print(client)


if __name__ == '__main__':
    client_db = Clients('<database>', '<username>', '<user password>', '<host>')
    
    # создание структуры базы данных
    client_db.create_db()
    
    # добавление нового клиента
    client1_id = client_db.add_client('Назар', 'Назаров')
    client_db.add_client('Владимир', 'Иванов', 'ivanov@mail.server.ru')
    client_db.add_client('Владимир', 'Васечкин', 'v.vas@mail.com', ['+1-111-111-1111', '(495)000-00-00', '+0(000) 000 00 00'])
    print('Сведения о добавленных клиентах:')
    client_db.print_all_clients()
    print()

    # добавление телефона для существующего клиента
    client_db.add_phone(client1_id, '1234567890')
    print('Сведения о клиентах после добавления телефона первому клиенту:')
    client_db.print_all_clients()
    print()

    # изменние данные о клиенте
    client_db.change_client(client1_id, first_name='Назар Назарович')
    client_db.change_client(client1_id, phones=['+1-234-567-8989', '(499)000-00-00'])
    print('Сведения о клиентах после изменения данных первого клиента:')
    client_db.print_all_clients()
    print()

    # удаление телефона для существующего клиента
    client_db.delete_phone(1,'(499)000-00-00')
    print('Сведения о клиентах после удаления телефона существующего клиента:')
    client_db.print_all_clients()
    print()

    # удаление существующего клиента
    client_db.delete_client(client1_id)
    print('Сведения о клиентах после удаления первого клиента:')
    client_db.print_all_clients()
    print()

    # демонстрация работы функции, позволяющей найти клиента по его данным: имени, фамилии, email или телефону
    search_result = client_db.find_client(first_name='Владимир')
    print(f'Результаты поиска клиента по имени (Владимир):\n{search_result}\n')
    search_result = client_db.find_client(first_name='Владимир', phone='+1-111-111-1111')
    print(f'Результаты поиска клиента по имени (Владимир) и телефону (+1-111-111-1111):\n{search_result}\n')
    search_result = client_db.find_client(email='ivanov@mail.server.ru')
    print(f'Результаты поиска клиента по email (ivanov@mail.server.ru):\n{search_result}\n')
