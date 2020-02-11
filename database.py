import psycopg2


class Database:
    def __init__(self, db='accidents', user='postgres', password='admin'):
        self.connection = psycopg2.connect(dbname=db, user=user, password=password)
        self.cursor = self.connection.cursor()
        self.cursor.execute('SET search_path="public"')
        self.connection.commit()

    def is_connected(self):
        try:
            self.cursor.execute('SELECT 1')
        except psycopg2.OperationalError:
            return False
        return True

    def insert_list(self, sql, values):
        keys = []
        for l in values:
            self.cursor.execute(sql, l)
            if self.cursor.description is not None:
                keys.append(self.cursor.fetchone()[0])
        self.connection.commit()
        return keys

    def query(self, sql, values):
        if values is None:
            self.cursor.execute(sql)
            self.connection.commit()
            return
        self.cursor.execute(sql, values)
        self.connection.commit()
        try:
            return [e[0] for e in self.cursor.fetchall()]
        except psycopg2.OperationalError:
            return None

    def delete_all(self):
        self.cursor.execute('DROP TABLE IF EXISTS accident, event, fact_table, location, weather, hour;')
        self.connection.commit()
