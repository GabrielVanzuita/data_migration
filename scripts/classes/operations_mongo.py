import requests
import os
import json
import csv

class DataSource:
    def __init__(self, source, db_name, collection_name, connector):
        """
        Initializes the data handling and database connection.
        :param source: Source URL or local file path
        :param db_name: MongoDB database name
        :param collection_name: MongoDB collection name
        :param connector: MongoDB connector/client
        """
        self.source = source
        self.db_name = db_name
        self.collection_name = collection_name
        self.connector = connector  # Receives MongoDB client
        self.collection = None
        self.data_recon = None
        self.data_type = None
        self.data_url = None
        self.data_path = None
        self.data_print = None

    def handle_source(self):
        """
        Identifies whether the source is a URL or a local file.
        """
        if self.source.startswith("https:"):
            try:
                response = requests.get(self.source)
                response.raise_for_status()  # Checks for request errors
                self.data_recon = "online"
                self.data_url = response
                print("Online source detected and successfully loaded.")
            except requests.exceptions.RequestException as e:
                print(f"Error loading online source: {e}")
                self.data_recon = None
        elif os.path.exists(self.source):  # Checks if the source exists locally
            self.data_recon = "local"
            self.data_path = self.source
            print("Local file detected.")
        else:
            print("Invalid source: neither a URL nor a local file.")
            self.data_recon = None

    def detect_data_type(self):
        """
        Detects the data type (JSON or CSV) for either local or online sources.
        :returns: Data type (json, csv, or unknown)
        """
        if self.data_recon == "local":
            try:
                with open(self.source, 'r') as file:
                    # Tries to load as JSON
                    try:
                        json.load(file)
                        self.data_type = "json"
                        print("Local JSON file detected.")
                        return self.data_type
                    except json.JSONDecodeError:
                        file.seek(0)
                        try:
                            # Checks if it's a valid CSV
                            csv.Sniffer().sniff(file.read(1024))
                            file.seek(0)
                            self.data_type = "csv"
                            print("Local CSV file detected.")
                            return self.data_type
                        except csv.Error:
                            self.data_type = "unknown"
                            print("Unknown or unsupported file format.")
                            return self.data_type
            except FileNotFoundError:
                print("File not found.")
                self.data_type = None
                return self.data_type

        elif self.data_recon == "online":
            try:
                content = self.data_url.text
                # Tries to load as JSON
                try:
                    json.loads(content)
                    self.data_type = "json"
                    print("Online JSON source detected.")
                    return self.data_type
                except json.JSONDecodeError:
                    # Checks if it's CSV
                    try:
                        csv.Sniffer().sniff(content[:1024])
                        self.data_type = "csv"
                        print("Online CSV source detected.")
                        return self.data_type
                    except csv.Error:
                        self.data_type = "unknown"
                        print("Unknown or unsupported online source.")
                        return self.data_type
            except Exception as e:
                print(f"Error determining online data type: {e}")
                self.data_type = None
                return self.data_type
        else:
            print("Data type detection could not be performed.")
            self.data_type = None
            return self.data_type

    def create_db(self):
        """
        Creates the MongoDB database and collection.
        """
        try:
            db = self.connector[self.db_name]
            self.collection = db[self.collection_name]
            print(f"Database '{self.db_name}' and collection '{self.collection_name}' are ready.")
        except Exception as e:
            print(f"Error creating database or collection: {e}")

    def load_Data(self):
        """
        Loads data into MongoDB based on source type and file format (JSON or CSV).
        """
        try:
            db = self.connector[self.db_name]
            collection = db[self.collection_name]

            if self.data_recon == "online":
                content = self.data_url.text
                if self.data_type == "json":
                    data_list = json.loads(content)
                    if isinstance(data_list, list):
                        result = collection.insert_many(data_list)
                        print(f"{len(result.inserted_ids)} documents inserted successfully.")
                    else:
                        raise ValueError("The expected JSON format is a list of objects.")
                elif self.data_type == "csv":
                    reader = csv.DictReader(content.splitlines())
                    data = [row for row in reader]
                    result = collection.insert_many(data)
                    print(f"{len(result.inserted_ids)} documents inserted successfully.")
                else:
                    print("Unsupported online data type.")
            elif self.data_recon == "local":
                if self.data_type == "json":
                    with open(self.source, 'r') as file:
                        data = json.load(file)
                        result = collection.insert_many(data)
                        print(f"{len(result.inserted_ids)} documents inserted successfully.")
                elif self.data_type == "csv":
                    with open(self.source, mode='r') as file:
                        reader = csv.DictReader(file)
                        data = [row for row in reader]
                        result = collection.insert_many(data)
                        print(f"{len(result.inserted_ids)} documents inserted successfully.")
                else:
                    print("Unsupported local data type.")
        except Exception as e:
            print(f"Error loading data: {e}")

    def write_data(self):
        """
        Retrieves and prints all documents from the MongoDB collection.
        """
        paper = []
        db = self.connector[self.db_name]
        collection = db[self.collection_name]
        printer = list(collection.find())  # Correct: `find()` not `findclear()`
        paper.extend(printer)  # Appends all documents to the list `paper`
        self.data_print = paper  # Stores `paper` directly in `self.data_print`
        for doc in self.data_print:
            print(doc)

    def list_collections(self):
        """
        Lists available collections in the database.
        :returns: List of collection names
        """
        return self.db.list_collection_names()

    def get_columns(self, collection_name, limit=100):
        """
        Returns the names of the columns (fields) in a collection.
        :param collection_name: The collection name
        :param limit: Limit the number of documents to inspect
        :returns: List of column names
        """
        all_fields = set()
        collection = self.db[collection_name]
        for document in collection.find().limit(limit):
            all_fields.update(document.keys())
        return list(all_fields)
    

    # Getter and Setter methods for the class attributes
    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, value):
        self._source = value

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, value):
        self._db_name = value

    @property
    def collection_name(self):
        return self._collection_name

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value

    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value

    @property
    def collection(self):
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = value

    @property
    def data_recon(self):
        return self._data_recon

    @data_recon.setter
    def data_recon(self, value):
        self._data_recon = value

    @property
    def data_type(self):
        return self._data_type

    @data_type.setter
    def data_type(self, value):
        self._data_type = value

    @property
    def data_url(self):
        return self._data_url

    @data_url.setter
    def data_url(self, value):
        self._data_url = value

    @property
    def data_path(self):
        return self._data_path

    @data_path.setter
    def data_path(self, value):
        self._data_path = value

    @property
    def data_print(self):
        return self._data_print

    @data_print.setter
    def data_print(self, value):
        self._data_print = value
