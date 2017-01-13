import MySQLdb as mdb
import csv

class DB(object):
    def __init__(self):
        self.connection = None

    def connect(self):
        if self.connection is not None:
            return
        try:
            self.connection = mdb.connect('localhost', 'root', '1111', 'mydb')

        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            self.connection = None

    def close(self):
        if self.connection is not None:
            self.connection.close()
        self.connection = None

    def initialization(self):
        self.connect()
        if self.connection is None:
            return []

        cur = self.connection.cursor(mdb.cursors.DictCursor)

        cur.execute("DELETE FROM sale")
        cur.execute("ALTER TABLE sale AUTO_INCREMENT = 1")
        cur.execute("commit")

        cur.execute("DELETE FROM car")
        cur.execute("ALTER TABLE car AUTO_INCREMENT = 1")
        cur.execute("commit")

        cur.execute("DELETE FROM client")
        cur.execute("ALTER TABLE client AUTO_INCREMENT = 1")
        cur.execute("commit")

        cur.execute("DELETE FROM departmen")
        cur.execute("ALTER TABLE departmen AUTO_INCREMENT = 1")
        cur.execute("commit")

        with open('files/car.csv', 'rb') as f:
            cars = csv.reader(f, delimiter=';', quotechar='|')
            for car in cars:
                cur.execute("INSERT INTO car(name, model, year, number) VALUES (%s, %s, %s, %s)",
                            (car[0], car[1], car[2], car[3]))
                cur.execute("commit")

        with open('files/client.csv', 'rb') as f:
            clients = csv.reader(f, delimiter=';', quotechar='|')
            for client in clients:
                cur.execute("INSERT INTO client(name, secondname, number) VALUES (%s, %s, %s)",
                            (client[0], client[1], client[2]))
                cur.execute("commit")

        with open('files/departmen.csv', 'rb') as f:
            departmens = csv.reader(f, delimiter=';', quotechar='|')
            for dep in departmens:
                cur.execute("INSERT INTO departmen(addres, days) VALUES (%s, %s)",
                            (dep[0], dep[1]))
                cur.execute("commit")

        with open('files/sale.csv', 'rb') as f:
            sales = csv.reader(f, delimiter=';', quotechar='|')
            for sale in sales:
                cur.execute("INSERT INTO sale (idcar, idclient, iddepartmen, price, data)"
                            " VALUES(%s, %s, %s, %s, %s)",
                            (int(sale[0]), int(sale[1]), int(sale[2]), int(sale[3]), sale[4]))
                cur.execute("commit")
        self.close()

    def getGistList(self, table):
        self.connect()
        if self.connection is None:
            return []
        if not (table in ['car', 'client', 'departmen', 'sale']):
            return []
        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute("SELECT * FROM %s" % table)
        self.close()
        return cur.fetchall()

    def getGist(self, table, id):
        self.connect()
        if self.connection is None:
            return []
        if not (table in ['car', 'client', 'departmen', 'sale']):
            return []
        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute("SELECT * FROM %s WHERE %s.id%s = %d" % (table, table, table, int(id)))
        self.close()
        return cur.fetchall()

    def insertSale(self, idcar, idclient, iddepartmen, price, data):
        self.connect()
        if self.connection is None:
            return []
        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute("INSERT INTO sale (idcar, idclient, iddepartmen, price, data)"
                    " VALUES(%s, %s, %s, %s, %s)",
                    (int(idcar), int(idclient), int(iddepartmen), int(price), data))
        cur.execute("commit")
        self.close()

    def updateSale(self, idsale, idcar, idclient, iddepartmen, price, data):
        self.connect()
        if self.connection is None:
            return []
        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute("UPDATE sale SET idcar=%s, idclient=%s, "
                    "iddepartmen=%s, price=%s, data=%s where sale.idsale=%s",
                    (int(idcar), int(idclient), int(iddepartmen), int(price), data, int(idsale)))
        cur.execute("commit")
        self.close()

    def removeSale(self, id):
        self.connect()
        if self.connection is None:
            return []
        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute("DELETE FROM sale WHERE sale.idsale = %s" % int(id))
        cur.execute("commit")
        self.close()

    def getSaleFull(self):
        self.connect()
        if self.connection is None:
            return []

        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute(
            "SELECT sale.idsale, car.name, car.model, car.year, car.number,"
            " client.name as cn, client.secondname as cs, client.number as cnum,"
            " departmen.addres, departmen.days, sale.price, sale.data"
            " FROM sale, car, client, departmen"
            " WHERE sale.idcar = car.idcar"
            " AND sale.idclient = client.idclient"
            " AND sale.iddepartmen = departmen.iddepartmen")
        self.close()
        return cur.fetchall()

    def getSaleListByPrice(self, fromPrice, toPrice):
        self.connect()
        if self.connection is None:
            return []

        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute(
            "SELECT sale.idsale, car.name, car.model, car.year, car.number, "
            " client.name as cn, client.secondname as cs, client.number as cn,"
            " departmen.addres, departmen.days, sale.price, sale.data"
            " FROM sale, car, client, departmen"
            " WHERE sale.idcar = car.idcar"
            " and sale.idclient = client.idclient"
            " and sale.iddepartmen = departmen.iddepartmen"
            " and sale.price BETWEEN %s AND %s"
            % (fromPrice, toPrice))
        self.close()
        return cur.fetchall()

    def getSaleListByCarID(self, idcar):
        self.connect()
        if self.connection is None:
            return []

        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute(
            "SELECT sale.idsale, car.name, car.model, car.year, car.number, "
            " client.name as cn, client.secondname as cs, client.number as cn,"
            " departmen.addres, departmen.days, sale.price, sale.data"
            " FROM sale, car, client, departmen"
            " WHERE sale.idcar = car.idcar"
            " and sale.idclient = client.idclient"
            " and sale.iddepartmen = departmen.iddepartmen"
            " and car.idcar = %s " % idcar)
        self.close()
        return cur.fetchall()

    def getListExcluded(self, phrase):
        self.connect()
        if self.connection is None:
            return []

        cur = self.connection.cursor(mdb.cursors.DictCursor)
        cur.execute(
            "SELECT sale.idsale, car.name, car.model, car.year, car.number, "
            " client.name as cn, client.secondname as cs, client.number as cn,"
            " departmen.addres, departmen.days, sale.price, sale.data"
            " FROM sale, car, client, departmen"
            " WHERE sale.idcar = car.idcar"
            " and sale.idclient = client.idclient"
            " and sale.iddepartmen = departmen.iddepartmen"
            " and (MATCH (car.model) AGAINST ('\"%s\"' IN BOOLEAN MODE))" % phrase)
        self.close()
        return cur.fetchall()

    def getListIncluded(self, phrase):
        self.connect()
        if self.connection is None:
            return []

        cur = self.connection.cursor(mdb.cursors.DictCursor)
        newphrase = ""
        for str in phrase.split(" "):
            newphrase = newphrase + " +" + str
            print newphrase

        cur.execute(
            "SELECT sale.idsale, car.name, car.model, car.year, car.number, "
            " client.name as cn, client.secondname as cs, client.number as cn,"
            " departmen.addres, departmen.days, sale.price, sale.data"
            " FROM sale, car, client, departmen"
            " WHERE sale.idcar = car.idcar"
            " and sale.idclient = client.idclient"
            " and sale.iddepartmen = departmen.iddepartmen"
            " and (MATCH (car.name) AGAINST ('%s' IN BOOLEAN MODE))" % newphrase)
        self.close()
        return cur.fetchall()

