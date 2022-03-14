import datetime
import os


def get_connection():
    params = get_connection_params()
    connection = psycopg2.connect(**params)
    return connection


class DB:
    async def query(self, sql, *, params=None, select=True, one=False, last=False, value=False, model=None):
        con = get_connection()
        cur = con.cursor()
        try:
            cur.execute(sql, params)
            if select:
                columns = [col.name for col in cur.description]
                rows = cur.fetchall()
                if one:
                    item = (rows[-1] if last else rows[0]) if rows else []
                    if value:
                        data = item[0] if item else None
                    else:
                        data = {pair[0]: pair[1] for pair in list(zip(columns, item))}
                        if model:
                            data = model(**data)
                else:
                    if model:
                        data = [model(**{pair[0]: pair[1] for pair in list(zip(columns, item))}) for item in rows]
                    else:
                        data = [{pair[0]: pair[1] for pair in list(zip(columns, item))} for item in rows]
                return data
            else:
                cur.commit()
        except Exception as e:
            raise Exception(e)
        finally:
            cur.close()
            con.close()

    async def select(self, sql, params=None, model=None):
        if not params:
            params = []
        data = await self.query(sql, params=params, model=model)
        return data

    async def row(self, sql, params=None, model=None, last=False):
        if not params:
            params = []
        data = await self.query(sql, params=params, one=True, model=model, last=last)
        return data

    async def value(self, sql, params=None):
        v = await self.query(sql, params=params, one=True, value=True)
        return v or 0

    async def execute(self, sql, params=None):
        if not params:
            params = []
        ret = await self.query(sql, params=params, select=False)
        return ret

    async def list(self, table, *, where=None, order=None, model=None):
        if not where:
            where = {}
        if not order:
            order = {}
        sql = self._make_query_sql(table, where=where, order=order)
        data = await self.select(sql, model=model)
        return data

    async def item(self, table, pk, *, key='id', model=None):
        where = {key: pk}
        sql = self._make_query_sql(table, where=where, order={})
        item = await self.row(sql, model=model)
        return item

    async def count(self, table, *, where=None):
        if not where:
            where = {}
        sql = self._make_query_sql(table, where=where, order={})
        c = await self.value(sql)
        return c

    async def insert(self, table, *, data=None, model=None):
        sql = self.make_insert_sql(table, data)
        data = await self.query(sql, one=True, model=model)
        return data

    async def update(self, table, *, key='id', data=None, model=None):
        sql = self.make_update_sql(table, data, where={key: data[key]})
        data = await self.query(sql, one=True, model=model)
        return data

    async def change(self, table, *, data=None, where=None, model=None):
        sql = self.make_update_sql(table, data, where=where)
        data = await self.query(sql, one=True, model=model)
        return data

    def _make_query_sql(self, table, where, order):
        where_sql = self._make_where_section(where)
        order_sql = self._make_order_section(order)
        sql = f"""select * from {table} {where_sql} {order_sql}"""
        return sql

    def make_update_sql(self, table, data, where):
        sql = ''
        return sql

    def make_insert_sql(self, table, data):
        columns = ','.join(data.keys())
        values = ','.join([self._make_value(v) for k, v in data.items()])
        sql = f"""insert into {table}({columns}) values({values})"""
        return sql

    def _make_where_section(self, where):
        sql = 'and'.join([f"{k} = {self._make_value(v)}" for k, v in where.items()])
        if sql:
            sql = f"where {sql}"
        return sql

    def _make_order_section(self, order):
        sql = ''
        return sql

    def _make_value(self, value):
        if isinstance(value, (int, float)):
            return f"{value}"
        if isinstance(value, datetime.datetime):
            return f"'{value.isoformat()}'"
        if isinstance(value, datetime.date):
            return f"'{value.strftime('%Y-%m-%d')}'"
        return f"'{value}'"
