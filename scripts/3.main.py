import os
from classes.connection_mongo import MongoConnection
from scripts.classes.operations_mongo import DataSource

# Usar variáveis de ambiente para maior segurança
username = os.getenv("MONGO_USERNAME", "gbrvbusiness")
password = os.getenv("MONGO_PASSWORD", "9AHmX4Hna0nvuANa")
clustername = os.getenv("MONGO_CLUSTERNAME", "StudiesCluster")
data_source = r"/home/gbrv_linux/pipeline-python-mongo-mysql/scripts/pomodorojson.json"
db_name = "pasta"
collection_name = "pomodoro"

mg_Con = MongoConnection (username=username, password=password, clustername=clustername)
direct_con = mg_Con.connection()
data = DataSource(source=data_source, db_name=db_name, collection_name=collection_name)


#Teste de conexão
direct_con
mg_Con.list_databases()
mg_Con.close()

#Teste de leitura de arquivo
data.handle_source()







