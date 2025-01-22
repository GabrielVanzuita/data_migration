import requests
import os
import json
import csv

class DataSource:
    def __init__(self, source, db_name, collection_name, client):
        """Inicializa a manipulação de dados e banco de dados."""
        self.source = source
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = client  # Recebe o cliente do MongoDB
        self.collection = None
        self.data_recon = None
        self.data_type = None

    def handle_source(self):
        """Identifica se a fonte é uma URL ou um arquivo local."""
        if self.source.startswith("https:"):
            try:
                response = requests.get(self.source)
                response.raise_for_status()
                self.data_recon = "online"
                print("Fonte online detectada e carregada com sucesso.")
            except requests.exceptions.RequestException as e:
                print(f"Erro ao carregar a fonte online: {e}")
                self.data_recon = None
        elif os.path.exists(self.source):
            self.data_recon = "local"
            print("Arquivo local detectado.")
        else:
            print("Fonte inválida: não é uma URL ou arquivo local.")
            self.data_recon = None

    def detect_data_type(self):
        """Detecta se o arquivo local é JSON ou CSV."""
        if self.data_recon == "local":
            try:
                with open(self.source, 'r') as file:
                    try:
                        json.load(file)
                        self.data_type = "JSON"
                    except json.JSONDecodeError:
                        file.seek(0)
                        try:
                            csv.reader(file).__next__()
                            self.data_type = "CSV"
                        except (csv.Error, StopIteration):
                            self.data_type = "Unknown"
            except FileNotFoundError:
                print("Arquivo não encontrado.")
                self.data_type = None
        else:
            print("A detecção de tipo de dados é apenas para arquivos locais.")

    def create_db(self):
        """Cria ou conecta ao banco de dados e à coleção."""
        try:
            db = self.client[self.db_name]
            self.collection = db[self.collection_name]
            print(f"Banco de dados '{self.db_name}' e coleção '{self.collection_name}' prontos.")
        except Exception as e:
            print(f"Erro ao criar banco ou coleção: {e}")

    def load_data(self):
        """Carrega os dados na coleção do MongoDB."""
        if not self.collection:
            print("Coleção não definida. Execute 'create_db' primeiro.")
            return

        if self.data_recon == "online":
            try:
                response = requests.get(self.source)
                response.raise_for_status()
                docs = self.collection.insert_many(response.json())
                print(f"{len(docs.inserted_ids)} documentos adicionados da fonte online.")
            except Exception as e:
                print(f"Erro ao carregar dados online: {e}")
        elif self.data_recon == "local":
            if self.data_type == "JSON":
                try:
                    with open(self.source, 'r') as file:
                        docs = self.collection.insert_many(json.load(file))
                        print(f"{len(docs.inserted_ids)} documentos adicionados do arquivo JSON.")
                except Exception as e:
                    print(f"Erro ao carregar dados JSON: {e}")
            elif self.data_type == "CSV":
                try:
                    with open(self.source, 'r') as file:
                        reader = csv.DictReader(file)
                        docs = self.collection.insert_many(list(reader))
                        print(f"{len(docs.inserted_ids)} documentos adicionados do arquivo CSV.")
                except Exception as e:
                    print(f"Erro ao carregar dados CSV: {e}")
            else:
                print("Formato de arquivo não suportado ou desconhecido.")
        else:
            print("Tipo de fonte não reconhecido. Não é possível carregar dados.")
