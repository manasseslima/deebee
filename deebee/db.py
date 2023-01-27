import asyncio
import datetime
from typing import Union, Any

from deebee.pool import Pool


__all__ = ['DB']


def identify_operator(
        key: str
) -> tuple[str, str, str]:
    """Identify what operator by key __ suffix.

    :param key:
    :return:
    """
    spt = key.split('__')
    op = '='
    code = ''
    name = spt[0]
    if len(spt) > 1:
        code = spt[1]
        if code == 'in':
            op = 'in'
        elif code == 'nin':
            op = 'not in'
        elif code == 'eq':
            op = '='
        elif code == 'neq':
            op = '<>'
        elif code == 'gt':
            op = '>'
        elif code == 'gte':
            op = '>='
        elif code == 'lt':
            op = '<'
        elif code == 'lte':
            op = '<='
        elif code in ['between', 'bw']:
            op = 'between'
        elif code in ['starts', 'st']:
            op = 'like'
        elif code in ['ends', 'ed']:
            op = 'like'
        elif code == ['contains', 'ct']:
            op = 'like'
    return name, op, code


def make_column_alias(column):
    spt = column.split(':')
    if len(spt) > 1:
        return f'{spt[0]} as "{spt[1]}"'
    return spt[0]


def make_columns_section(columns):
    if not columns:
        return '*'
    ret = ', '.join(make_column_alias(col) for col in columns)
    return ret


class DB:
    def __init__(self, pool=None):
        self.pool = pool or Pool()

    def __del__(self):
        self.pool.close()

    async def select(
            self,
            sql: str,
            *,
            params: Union[list, tuple] = None,
            model: any = None,
            timeout: int = None
    ) -> Union[list, tuple]:
        """Returns data list by query

        Execute a current select query passed by sql param. Each item by list is a dict
        representing a row returned by query. When the model param is set, each row will be a instance of this model.

        This returns a list of rows when have data and an empty list when nothing was found.
        :param sql:
        :param params:
        :param model:
        :param timeout:
        :return:
        """
        if not params:
            params = []
        data = await self.__query(sql, params=params, model=model, timeout=timeout)
        return data or (None if model else [])

    async def row(
            self,
            sql: str,
            *,
            params: Union[list, tuple] = None,
            model: any = None,
            last: bool = False,
            timeout=None
    ) -> Union[dict, any]:
        """Returns the data as dict

        Executes command sql passed by sql param and return just one registry. If query retrive more than one row,
        by default the first row is selected. The last row can be selected by setting true 'last' param.

        This return a dict type object. When nothing was returned by query, an empty dict is returned.
        :param sql:
        :param params:
        :param model:
        :param last:
        :param timeout:
        :return:
        """
        if not params:
            params = ()
        data = await self.__query(sql, params=params, one=True, model=model, last=last, timeout=timeout)
        return data or {}

    async def value(
            self,
            sql: str,
            *,
            params: Union[list, tuple] = None,
            timeout: int = None
    ) -> any:
        """Returns only one value.

        This execute query em return just the first column from first rows of the returned list by default.

        :param sql:
        :param params:
        :param timeout:
        :return:
        """
        v = await self.__query(sql, params=params, one=True, value=True, timeout=timeout)
        return v

    async def execute(
            self,
            sql: str,
            *,
            params=None,
            timeout=None
    ) -> any:
        """Execute a sql command.

        :param sql:
        :param params:
        :param timeout:
        :return:
        """
        if not params:
            params = []
        ret = await self.__query(sql, params=params, select=False, timeout=timeout)
        return ret

    async def get_list(
            self,
            table: str,
            *,
            columns: Union[list[str], tuple[str], str] = None,
            where: dict = None,
            order: Union[list[str], tuple[str], str] = None,
            page: int = 1,
            size: int = 20,
            model: any = None
    ) -> Union[list, tuple]:
        """Creates a query and return a list.

        This is a helper method that takes the table name, columns list and a constraint dict and generate
        a sql command. With this sql generated, the select method is called to retrive the data.

        The return is the value returned by select method.

        :param size:
        :param page:
        :param table:
        :param columns:
        :param where:
        :param order:
        :param model:
        :return:
        """
        if not where:
            where = {}
        if not order:
            order = ()
        if not columns:
            columns = ()
        sql = self.__generate_query_sql(table, columns=columns, where=where, order=order)
        if page:
            offset = (page - 1) * size
            sql = f'{sql} limit {size} offset {offset}'
        data = await self.select(sql, model=model)
        return data

    async def array(
            self,
            table: str,
            *,
            column: str = '',
            where: dict = None,
            order: Union[str, tuple, list] = '',
            model: any = None
    ) -> Union[list, tuple]:
        """Special method to get a list of values without keys. This get first column of each row retried.

        :param table:
        :param column:
        :param where:
        :param order:
        :param model:
        :return:
        """
        data = await self.get_list(table, columns=[column], where=where, order=order, model=model)
        data = [row[column] for row in data]
        return data

    async def get_item(
            self,
            table: str,
            *,
            pk: Union[str, int, float] = '',
            key: str = '',
            where: dict = None,
            model: any = None,
            order: Union[dict, list[str], tuple[str], str] = ''
    ) -> Union[dict, any]:
        """Return the item by query generated by arguments

        :param table:
        :param pk:
        :param key:
        :param where:
        :param model:
        :param order:
        :return:
        """
        if not where:
            where = {key: pk} if pk else {}
        sql = self.__generate_query_sql(table, where=where, order=order)
        item = await self.row(sql, model=model)
        return item

    async def count(
            self,
            table: str,
            *,
            where: dict = None
    ) -> any:
        """Return the number of rows affected.

        :param table:
        :param where:
        :return:
        """
        if not where:
            where = {}
        where_section = self.__generate_where_section(where=where)
        sql = f"""select count(*) as count from {table} {where_section}"""
        c = await self.value(sql)
        return 0 if c is None else c

    async def insert(
            self,
            table: str,
            *,
            data: Union[Any] = None,
            model: any = None
    ) -> Union[dict, any]:
        if not isinstance(data, (dict, list)):
            data = data.dict()
        sql = self.__generate_insert_command(table, data)
        data = await self.__query(sql, one=True, model=model)
        return data

    async def update(
            self,
            table: str,
            *,
            pk: Union[str, int, float] = '',
            key: Union[str, tuple, list] = '',
            data: Union[dict, list, tuple] = None,
            model: any = None
    ) -> Union[dict, any]:
        """Updates table with given data.

        :param table:
        :param key:
        :param data:
        :param model:
        :return:
        """
        keys_cols = key.split(',')
        if len(keys_cols) == 1 and pk != data.get(key, None):
            raise Exception('PK value must be same of data')
        if isinstance(data, dict):
            where = {k: data.pop(k) for k in keys_cols}
        else:
            where = {k: None for k in keys_cols}
        sql = self.__generate_update_command(table, data, where=where)
        data = await self.__query(sql, one=True, model=model)
        return data

    async def apply(
            self,
            table: str,
            *,
            key: Union[str, tuple, list] = '',
            sort: Union[str, tuple, list] = '',
            data: Union[dict, list] = None,
            model: any = None
    ):
        """Verify if each data is in table and choose if call update or insert.

        :param table:
        :param key:
        :param sort:
        :param data:
        :param model:
        :return:
        """
        if not data:
            return data
        if not key:
            raise Exception('The key column must be informed!')
        if isinstance(data, list):
            keys_cols = key.split(',') or data[0].keys()
            sort_cols = sort.split(',')
            where_key = {k: data[0][k] for k in keys_cols}
            where_sort = {f'{k}__in': [item[k] for item in data] for k in sort_cols}
            where = {**where_key, **where_sort}
            column = sort_cols[0] if sort_cols else keys_cols[0]
            array = self.array(table, column=column, where=where)
            update_data = []
            insert_data = []
            for item in data:
                if item[column] in array:
                    update_data.append(item)
                else:
                    insert_data.append(item)
            resp = await asyncio.gather(
                self.update(table, key=key, data=update_data, model=model),
                self.insert(table, data=insert_data, model=model)
            )
            return resp
        else:
            keys_cols = key.split(',') or data.keys()
            count = await self.count(table, where={k: data[k] for k in keys_cols})
            if count:
                data = await self.update(table, key=key, data=data, model=model)
            else:
                data = await self.insert(table, data=data, model=model)
            return data

    async def change(
            self,
            table: str,
            *,
            data: dict = None,
            where: dict = None, 
            db='', 
            model=None
    ) -> Union[dict, any]:
        """Update registry by conditions given.

        :param table:
        :param data:
        :param where:
        :param db:
        :param model:
        :return:
        """
        sql = self.__generate_update_command(table, data, where=where)
        data = await self.__query(sql, one=True, model=model)
        return data

    async def delete(
            self,
            table: str,
            *,
            key: str = '',
            pk: Union[str, int, float] = None
    ) -> Union[dict, any]:
        """Remove table row by given key.

        :param table:
        :param key:
        :param pk:
        :return:
        """
        where = {key: pk}
        sql = self.__generate_delete_command(table, where=where)
        item = await self.get_item(table, key=key, pk=pk)
        await self.execute(sql)
        return item

    def __generate_query_sql(
            self,
            table: str,
            columns: Union[list, str, tuple] = '*',
            where: dict = None,
            order: Union[str, dict, list, tuple] = ''
    ) -> str:
        """Generate sql query.

        :param table:
        :param columns:
        :param where:
        :param order:
        :return:
        """
        where = where or {}
        columns_sql = make_columns_section(columns)
        where_sql = self.__generate_where_section(where)
        order_sql = self.__generate_order_section(order)
        sql = f"""select {columns_sql} from {table} {where_sql} {order_sql}"""
        return sql

    def __generate_update_command(
            self,
            table: str,
            data: Union[dict, list],
            where: dict = None,
            output: str = '*'
    ) -> str:
        """Generate update command.

        :param table:
        :param data:
        :param where:
        :param output:
        :return:
        """
        if not data:
            return ''
        set_section = self.__generate_set_section(data)
        where_section = self.__generate_where_section(where, multi=isinstance(data, list))
        sql = f"update {table} as u set {set_section} {where_section} returning {output}"
        return sql

    def __generate_insert_command(
            self,
            table: str,
            data: Union[dict, list, tuple],
            output: str = '*'
    ) -> str:
        """Generate insert command

        :param table:
        :param data:
        :param output:
        :return:
        """
        if not data:
            return ''
        if isinstance(data, dict):
            data = [data]
        columns = ', '.join(data[0].keys())
        values_section = ','.join([f"({','.join([self.__build_value(v) for k, v in item.items()])})" for item in data])
        sql = f"""insert into {table}({columns}) values {values_section} returning {output}"""
        return sql

    def __generate_delete_command(
            self,
            table: str,
            where: dict
    ) -> str:
        """Generate delete command.

        :param table:
        :param where:
        :return:
        """
        where_section = self.__generate_where_section(where)
        sql = f"""delete from {table} {where_section}"""
        return sql

    def __generate_where_section(
            self,
            where: dict = None,
            multi=False
    ) -> str:
        """Generate where section.

        :param where:
        :param multi:
        :return:
        """
        if not where:
            return ''
        sql = ' and '.join([self.__mount_where_pair(k, v, multi) for k, v in where.items()])
        if sql:
            sql = f"where {sql}"
        return sql

    def __generate_set_section(
            self,
            data: dict = None
    ) -> str:
        """Generate set section.

        :param data:
        :return:
        """
        if not data:
            return ''
        if isinstance(data, dict):
            sql = ', '.join([f"{k} = {self.__build_value(v)}" for k, v in data.items()])
        else:
            columns = data[0].keys()
            set_columns_section = ', '.join(f"{k} = u2.{k}" for k in columns)
            values_section = ', '.join(f"({','.join([self.__build_value(v) for k, v in item.items()])})" for item in data)
            columns_section = ', '.join(k for k in columns)
            sql = f"{set_columns_section} from (values {values_section}) as u2({columns_section})"
        return sql

    def __generate_order_section(
            self,
            order: Union[str, dict, list, tuple] = ''
    ) -> str:
        """Generate order section.

        :param order:
        :return:
        """
        cols = order
        if isinstance(order, str):
            cols = order.split(',')
        sql = ','.join(cols)
        return sql

    def __build_value(
            self,
            value: any,
            code: str = ''
    ) -> str:
        """Return correct value by type.

        :param value:
        :param code:
        :return:
        """
        if code in ('starts', 'st'):
            return f"'{value}%'"
        if code in ('ends', 'ed'):
            return f"'%{value}'"
        if code in ('contains', 'ct'):
            return f"'%{value}%'"
        if code in ('between', 'bw'):
            if isinstance(value, (int, float)):
                return f"{value[0]} and {value[1]}"
            return f"'{value[0]}' and '{value[1]}'"
        if code in ('in', 'nin'):
            return f"""({f','.join((f"{v}" if isinstance(v, (int, float, bool)) else f"'{v}'" for v in value))})"""
        if isinstance(value, (int, float)):
            return f'{value}'
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, datetime.datetime):
            return f"'{value.isoformat()}'"
        if isinstance(value, datetime.date):
            return f"'{value.strftime('%Y-%m-%d')}'"
        if value is None:
            return 'null'
        if value.startswith('ST_'):
            return value
        if isinstance(value, str):
            value = value.replace("'", "''").replace("’", "''")
        return f"'{value}'"

    def __mount_where_pair(
            self,
            key: str,
            value: any,
            multi: bool = False
    ) -> str:
        """

        :param key:
        :param value:
        :param multi:
        :return:
        """
        name, operator, code = identify_operator(key)
        if multi:
            ret = f"u2.{name} {operator} u.{name}"
        else:
            ret = f"{name} {operator} {self.__build_value(value, code)}"
        return ret

    async def __query(
            self,
            sql,
            *,
            params=None,
            select=True,
            one=False,
            last=False,
            value=False,
            model=None,
            timeout=None
    ) -> Union[list, tuple, dict, any]:
        """Execute all queries mounted by class.

        :param sql:
        :param params:
        :param select:
        :param one:
        :param last:
        :param value:
        :param model:
        :param timeout:
        :return:
        """
        con = await self.pool.acquire()
        cur = await con.cursor()
        try:
            await cur.execute(sql, params, timeout=timeout)
            if select:
                columns = [col.name for col in cur.description]
                if one:
                    item = await cur.fetchone()
                    if value:
                        data = item[0] if item else None
                    else:
                        data = dict(zip(columns, item))
                        if model:
                            data = model(**data)
                else:
                    rows = await cur.fetchall()
                    if model:
                        data = [model(**{pair[0]: pair[1] for pair in list(zip(columns, item))}) for item in rows]
                    else:
                        data = [dict(zip(columns, item)) for item in rows]
                return data
            else:
                ...
        except Exception as e:
            raise Exception(e)
        finally:
            cur.close()
            await con.close()
