import pymysql
import random
from datetime import datetime

LOCALHOST = '127.0.0.1'
USER = "vlad"
PASSWORD = "scoobyandsookie"
PORT = 3306
DB = "application_db"


class MyApplication:
    ###TODO CONNECTION WITH CONTEXT MANAGER
    ###TODO READ THE DATA FROM A FILE WITH CONTEXT MANAGER FOR THE CONNECTION
    ###TODO ADD COMMENTS
    def __init__(self):
        self.connection = pymysql.connect(
            host=LOCALHOST,
            user=USER,
            passwd=PASSWORD,
            port=PORT,
            db=DB,
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True)
        self.cursor = self.connection.cursor()

    def create_table(self):
        """Create the table"""
        sql_create_table = """CREATE TABLE IF NOT EXISTS main_table  (
                    id INT NOT NULL AUTO_INCREMENT,
                    ts_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    r_value INT NOT NULL,
                    PRIMARY KEY (id)) AUTO_INCREMENT=1;"""
        self.cursor.execute(sql_create_table)
        for i in range(0, 300):
            now = datetime.now()
            formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
            value = random.randint(1000, 9999)
            sql = f"INSERT INTO main_table " \
                  f"(ts_created, r_value) " \
                  f"VALUES ('{formatted_date}', '{value}');"""
            self.cursor.execute(sql)

    def read_table(self):
        """Reads table and displays values greater than 9000"""
        sql = f"SELECT * " \
              f"FROM main_table WHERE r_value>9000"
        self.cursor.execute(sql)
        query_result = self.cursor.fetchall()
        print(query_result)


if __name__ == '__main__':
    my_application = MyApplication()
    my_application.create_table()
    my_application.read_table()


