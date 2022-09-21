import os

kind = os.environ.get('DEEBEE_TYPE', 'sqlite')
match kind:
    case 'sqlite':
        from deebee.connectors import sqlite as connector
    case 'postgresql':
        from deebee.connectors import postgresql as connector
    case 'mysql':
        from deebee.connectors import mysql as connector


class Cursor:
    def close(self):
        ...


class Connection:
    def __init__(self, pool=None):
        self.con = None
        self.pool = pool
        self.closed: bool = False

    async def initialize(self):
        self.con = await connector.get_connection()

    async def cursor(self):
        return await self.con.cursor()

    async def close(self):
        await self.con.close()
        self.closed = True
