import pymysql
import random
from datetime import datetime
from contextlib import contextmanager
import json


class MyApplication:
    def __init__(self):
        with open('data.json') as f:
            sensitive_data = json.load(f)
            self.host = sensitive_data['LOCALHOST']
            self.user = sensitive_data['USER']
            self.passwd = sensitive_data['PASSWORD']
            self.port = sensitive_data['PORT']
            self.db = sensitive_data['DB']
        self.charset = 'utf8'
        self.cursorclass = pymysql.cursors.DictCursor
        self.autocommit = True

    @contextmanager
    def get_connection(self):
        """Context manager for the database connection"""
        connection = pymysql.connect(host=self.host, user=self.user,
                                     passwd=self.passwd, port=self.port,
                                     db=self.db, charset=self.charset,
                                     cursorclass=self.cursorclass,
                                     autocommit=self.autocommit)
        try:
            yield connection
        finally:
            connection.close()

    def create_table(self):
        """Method that creates and populates the table with the desired data"""
        create_table_query = """CREATE TABLE IF NOT EXISTS main_table  (
                    id INT NOT NULL AUTO_INCREMENT,
                    ts_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    r_value INT NOT NULL,
                    PRIMARY KEY (id)) AUTO_INCREMENT=1;"""

        with self.get_connection() as con:
            with con.cursor() as cursor:
                cursor.execute(create_table_query)
                for _ in range(300):
                    now = datetime.now()
                    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
                    value = random.randint(1000, 9999)
                    insert_query = f"INSERT INTO main_table " \
                                   f"(ts_created, r_value) " \
                                   f"VALUES ('{formatted_date}', '{value}');"
                    cursor.execute(insert_query)

    def read_table(self):
        """Reads table and displays values greater than 9000"""
        sql = """SELECT * FROM main_table WHERE r_value>9000"""
        with self.get_connection() as con:
            with con.cursor() as cursor:
                cursor.execute(sql)
                query_result = cursor.fetchall()
                print(query_result)


my_application = MyApplication()
my_application.create_table()
my_application.read_table()
