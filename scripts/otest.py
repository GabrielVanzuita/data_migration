import os
from classes.connection_mongo import MongoConnection
from classes.operations import DataSource
import requests

# Usar variáveis de ambiente para maior segurança
username = os.getenv("MONGO_USERNAME", "gbrvbusiness")
password = os.getenv("MONGO_PASSWORD", "9AHmX4Hna0nvuANa")
clustername = os.getenv("MONGO_CLUSTERNAME", "StudiesCluster")
data_source = r"C:\Users\Gabriel\Desktop\pasta_pomodoro_recipes.csv"
db_name = "pasta"
collection_name = "pomodoro"

#mg_Con = MongoConnection (username=username, password=password, clustername=clustername)
#direct_con = mg_Con.connection()
data = DataSource(source=data_source, db_name=db_name, collection_name=collection_name)


#Teste de conexão
#direct_con
#mg_Con.list_databases()
#mg_Con.close()

#Teste de leitura de arquivo
#data.handle_source()

data_source2 = r"C:\Users\Gabriel\Desktop\Study projects\pasta_pomodoro_recipes.json"

print(f"Diretório atual: {os.getcwd()}")
print(f"Caminho fornecido: {data_source2}")

# Verificar existência do caminho
print(os.path.exists(r"C:\Users\Gabriel\Desktop\Study projects\pasta_pomodoro_recipes.json"))