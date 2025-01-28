import mysql.connector
from mysql.connector import Error

class MySQLCon:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        """Estabelece a conexão com o MySQL sem acessar um banco de dados específico."""
        try:
            # Estabelece a conexão com o MySQL
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.username,
                password=self.password
            )
            if self.connection.is_connected():
                # Cria o cursor da conexão
                self.cursor = self.connection.cursor()
                print("Conexão com o MySQL bem-sucedida.")
                return self.connection
        except Error as e:
            print(f"Erro ao conectar ao MySQL: {e}")
            return None

    def use_database(self, db):
        """Seleciona um banco de dados específico após a conexão."""
        if not self.connection or not self.connection.is_connected():
            print("Nenhuma conexão ativa.")
            return None
        try:
            # Tenta usar o banco de dados especificado
            self.cursor.execute(f"USE {db}")
            print(f"Banco de dados '{db}' selecionado.")
        except Error as e:
            print(f"Erro ao acessar o banco de dados '{db}': {e}")
            return None

    def close_con(self):
        """Fecha a conexão e o cursor com o MySQL."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print("Conexão encerrada com sucesso.")
        except Exception as e:
            print(f"A conexão não foi encerrada. Erro: {e}")

    
    