

class Cursor:
    def close(self):
        ...


class Connection:

    def cursor(self):
        return Cursor()

    def close(self):
        ...
