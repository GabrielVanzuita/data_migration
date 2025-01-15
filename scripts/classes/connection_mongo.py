from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus

class MongoConnection:
    def __init__(self, username, password, clustername):
        """Inicializa a conexão MongoDB com credenciais."""
        self.username = username
        self.password = quote_plus(password)  # Escapando a senha
        self.clustername = clustername
        self.client = None  # Inicializa o cliente como None

    def connection(self):
        """Estabelece a conexão com o MongoDB."""
        uri = (
            f"mongodb+srv://{self.username}:{self.password}@{self.clustername}.whc9v.mongodb.net/"
            "?retryWrites=true&w=majority"
        )
        try:
            self.client = MongoClient(uri, server_api=ServerApi('1'))
            self.client.admin.command('ping')  # Testa a conexão
            print("Conexão com o MongoDB estabelecida com sucesso.")
            return self.client
        except Exception as e:
            print(f"Erro ao conectar: {e}")
            return None

    def list_databases(self):
        """Lista os bancos de dados disponíveis."""
        if not self.client:
            print("Nenhuma conexão ativa.")
            return
        try:
            dbs = self.client.list_database_names()
            print(f"Bancos de dados disponíveis: {dbs}")
        except Exception as e:
            print(f"Erro ao listar os bancos de dados: {e}")

    def close(self):
        """Fecha a conexão com o MongoDB."""
        if self.client:
            self.client.close()
            print("Conexão encerrada.")
