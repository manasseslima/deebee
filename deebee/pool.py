from deebee.connection import Connection


class Pool:
    def __init__(self):
        self.connections = {}
        self.waiting = []
        self.running = []

    async def acquire(self):
        if self.waiting:
            con = self.waiting.pop()
        else:
            con = Connection(pool=self)
            await con.initialize()
        return con

    def close(self):
        ...


