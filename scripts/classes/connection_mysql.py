import mysql.connector

class MySQLCon:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        """Estabelece a conexão com o MySQL e cria um cursor."""
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.username,
            password=self.password
        )
        self.cursor = self.connection.cursor()

    def close_con(self):
        """Fecha a conexão e o cursor com o MySQL."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()



    
    