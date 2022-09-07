from deebee.connection import Connection


class Pool:
    def get_connection(self):
        con = Connection()
        return con

    def close(self):
        ...
