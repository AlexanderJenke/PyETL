import psycopg2 as db
import torch


class OMOP(torch.utils.data.Dataset):
    def __init__(self,
                 dbname='OHDSI',
                 user='ohdsi_admin_user',
                 host='localhost',
                 port='5432',
                 password='omop'):
        self.conn = db.connect(f"dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{password}'")
        self.cursor = self.conn.cursor()
        self.data = self._prepare_data()

    def _prepare_data(self):
        res = self._select("""SELECT *
                              FROM p21_cdm.person""")
        data = []
        return data

    def __getitem__(self, item):
        return self.data[item]

    def __len__(self):
        return len(self.data)

    def _select(self, sql: str):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


if __name__ == '__main__':
    pass
