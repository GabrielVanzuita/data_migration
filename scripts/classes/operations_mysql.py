class mySQLoperator:
    def __init__(self, operations_mongo, db_name, connector, table_name, columns):
        """
        Inicializa a manipulação de dados e banco de dados.

        :param operations_mongo: Instância da classe mongoOperator
        :param db_name: Nome do banco de dados MySQL
        :param collection_name: Nome da coleção/tabela MySQL
        :param connector: Conexão com o banco de dados MySQL
        """
        self.operations_mongo  = operations_mongo  # Instância da classe mongoOperator
        self.connector = connector  # Conexão MySQL
        self.db_name = db_name  # Nome do banco
        self.table_name = table_name
        self.columns = columns
        self.data_print = operations_mongo.data_print  # Dados do MongoDB
    
    def create_database(self):
        """
        Cria o banco de dados, caso não exista.
        """
        cursor = self.connector.cursor()
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
            print(f"Banco de dados '{self.db_name}' criado ou já existente.")
        except Exception as e:
            print(f"Erro ao criar o banco de dados: {e}")
        finally:
            cursor.close()

    def create_table(self):
        """
        Cria a tabela no banco de dados MySQL.
        """
        cursor = self.connector.cursor()
        try:
            cursor.execute(f"USE {self.db_name}")  # Seleciona o banco de dados
            columns_def = ", ".join([f"{name} {col_type}" for name, col_type in self.columns])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns_def})"
            cursor.execute(create_table_query)
            print(f"Tabela '{self.table_name}' criada ou já existente.")
        except Exception as e:
            print(f"Erro ao criar a tabela: {e}")
        finally:
            cursor.close()

    def migrate_data(self):
        """
        Realiza a migração de dados do MongoDB para o MySQL.
        """
        if not self.data_print:
            print("Nenhum dado para migrar.")
            return

        cursor = self.connector.cursor()
        try:
            cursor.execute(f"USE {self.db_name}")  # Seleciona o banco de dados

            for document in self.data_print:
                # Converte os valores do documento para uma tupla
                keys = ", ".join(document.keys())
                placeholders = ", ".join(["%s"] * len(document))
                values = tuple(document.values())

                sql = f"INSERT INTO {self.table_name} ({keys}) VALUES ({placeholders})"
                cursor.execute(sql, values)

            self.connector.commit()
            print("Migração concluída com sucesso.")
        except Exception as e:
            print(f"Erro na migração de dados: {e}")
            self.connector.rollback()
        finally:
            cursor.close()

    # Getter e Setter para operations_mongo
    @property
    def operations_mongo(self):
        return self._operations_mongo

    @operations_mongo.setter
    def operations_mongo(self, value):
        self._operations_mongo = value

    # Getter e Setter para connector
    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

