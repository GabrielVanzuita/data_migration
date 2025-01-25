import requests
import os
import json
import csv


class DataSource:
    def __init__(self, source, db_name, collection_name, connector):
        """Inicializa a manipulação de dados e banco de dados."""
        self.source = source
        self.db_name = db_name
        self.collection_name = collection_name
        self.connector = connector  # Recebe o cliente do MongoDB
        self.collection = None
        self.data_recon = None
        self.data_type = None
        self.data_url = None
        self.data_path = None

    def handle_source(self):
        """Identifica se a fonte é uma URL ou um arquivo local."""
        if self.source.startswith("https:"):
            try:
                response = requests.get(self.source)
                response.raise_for_status()
                self.data_recon = "online"
                self.data_url = response
                print("Fonte online detectada e carregada com sucesso.")
            except requests.exceptions.RequestException as e:
                print(f"Erro ao carregar a fonte online: {e}")
                self.data_recon = None
        elif os.path.exists(self.source):
            self.data_recon = "local"
            self.data_path = self.source
            print("Arquivo local detectado.")
        else:
            print("Fonte inválida: não é uma URL ou arquivo local.")
            self.data_recon = None

    def detect_data_type(self):
        """Detecta o tipo de dados (JSON ou CSV) para fontes locais ou online."""
        if self.data_recon == "local":
            try:
                with open(self.source, 'r') as file:
                    # Tenta carregar como JSON
                    try:
                        json.load(file)
                        self.data_type = "json"
                        print("Arquivo local do tipo JSON detectado.")
                        return self.data_type
                    except json.JSONDecodeError:
                        file.seek(0)
                        try:
                            # Verifica se é um CSV válido
                            csv.Sniffer().sniff(file.read(1024))
                            file.seek(0)
                            self.data_type = "csv"
                            print("Arquivo local do tipo CSV detectado.")
                            return self.data_type
                        except csv.Error:
                            self.data_type = "unknown"
                            print("Arquivo desconhecido ou não suportado.")
                            return self.data_type
            except FileNotFoundError:
                print("Arquivo não encontrado.")
                self.data_type = None
                return self.data_type

        elif self.data_recon == "online":
            try:
                content = self.data_url.text
                # Tenta carregar como JSON
                try:
                    json.loads(content)
                    self.data_type = "json"
                    print("Fonte online do tipo JSON detectada.")
                    return self.data_type
                except json.JSONDecodeError:
                    # Verifica se é CSV
                    try:
                        csv.Sniffer().sniff(content[:1024])
                        self.data_type = "csv"
                        print("Fonte online do tipo CSV detectada.")
                        return self.data_type
                    except csv.Error:
                        self.data_type = "unknown"
                        print("Fonte online desconhecida ou não suportada.")
                        return self.data_type
            except Exception as e:
                print(f"Erro ao determinar o tipo de dados online: {e}")
                self.data_type = None
                return self.data_type
        else:
            print("A detecção de tipo de dados não pôde ser realizada.")
            self.data_type = None
            return self.data_type

    def create_db(self):
        try:
            db = self.connector[self.db_name]
            self.collection = db[self.collection_name]
            print(f"Banco de dados '{self.db_name}' e coleção '{self.collection_name}' prontos.")
        except Exception as e:
            print(f"Erro ao criar banco ou coleção: {e}")

    def load_Data(self):
        """Carrega os dados para o MongoDB com base no tipo de fonte e arquivo."""
        try:
            db = self.connector[self.db_name]
            collection = db[self.collection_name]

            if self.data_recon == "online":
                content = self.data_url.text
                if self.data_type == "json":
                    data_list = json.loads(content)
                    if isinstance(data_list, list):
                        result = collection.insert_many(data_list)
                        print(f"{len(result.inserted_ids)} documentos inseridos com sucesso.")
                    else:
                        raise ValueError("O formato do JSON esperado é uma lista de objetos.")
                elif self.data_type == "csv":
                    reader = csv.DictReader(content.splitlines())
                    data = [row for row in reader]
                    result = collection.insert_many(data)
                    print(f"{len(result.inserted_ids)} documentos inseridos com sucesso.")
                else:
                    print("Tipo de dados online não suportado.")
            elif self.data_recon == "local":
                if self.data_type == "json":
                    with open(self.source, 'r') as file:
                        data = json.load(file)
                        result = collection.insert_many(data)
                        print(f"{len(result.inserted_ids)} documentos inseridos com sucesso.")
                elif self.data_type == "csv":
                    with open(self.source, mode='r') as file:
                        reader = csv.DictReader(file)
                        data = [row for row in reader]
                        result = collection.insert_many(data)
                        print(f"{len(result.inserted_ids)} documentos inseridos com sucesso.")
                else:
                    print("Tipo de dados local não suportado.")
        except Exception as e:
            print(f"Erro ao carregar dados: {e}")
