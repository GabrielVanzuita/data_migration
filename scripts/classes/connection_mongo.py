from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus

class MongoConnection:
    def __init__(self, username, password, clustername):
        """
        Initializes the MongoDB connection with credentials.
        :param username: MongoDB username
        :param password: MongoDB password
        :param clustername: MongoDB cluster name
        """
        self.username = username
        self.password = quote_plus(password)  # Escapes the password to ensure special characters are encoded
        self.clustername = clustername
        self.client = None  # Initializes the MongoDB client as None, will be set during connection

    def connection(self):
        """
        Establishes the connection to MongoDB using the provided credentials.
        :returns: MongoDB client if the connection is successful, None otherwise.
        """
        uri = (
            f"mongodb+srv://{self.username}:{self.password}@{self.clustername}.whc9v.mongodb.net/"
            "?retryWrites=true&w=majority"
        )
        try:
            self.client = MongoClient(uri, server_api=ServerApi('1'))  # Connects to the MongoDB server using the URI
            self.client.admin.command('ping')  # Pings the MongoDB server to check the connection
            print("MongoDB connection established successfully.")
            return self.client
        except Exception as e:
            print(f"Error connecting: {e}")
            return None

    def list_databases(self):
        """
        Lists available databases in the MongoDB connection.
        :returns: None
        """
        if not self.client:
            print("No active connection.")
            return
        try:
            dbs = self.client.list_database_names()  # Fetches the list of database names
            print(f"Available databases: {dbs}")
        except Exception as e:
            print(f"Error listing databases: {e}")

    def close(self):
        """
        Closes the MongoDB connection.
        :returns: None
        """
        if self.client:
            self.client.close()  # Closes the MongoDB client connection
            print("Connection closed.")
