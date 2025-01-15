import requests
import os
import json
import csv

class DataSource:
    def __init__(self, source, db_name, collection_name):
        """Initialize with a data source."""
        self.source = source
        self.db_name = db_name
        self.collection_name = collection_name
        self.collection = None
        self.data_recon = None
        self.data_type = None

    def handle_source(self):
        """Identify if the source is an online URL or a local file."""
        if self.source.startswith("https:"):
            try:
                response = requests.get(self.source)
                response.raise_for_status()
                self.data_recon = "online"
                print("Online source detected and fetched successfully.")
            except requests.exceptions.RequestException as e:
                print(f"Error fetching online source: {e}")
                self.data_recon = None
        elif os.path.exists(self.source):
            self.data_recon = "local"
            print("Local file detected.")
        else:
            print("Invalid source: Not a valid URL or local file.")
            self.data_recon = None

    def detect_data_type(self):
        """Detect if the local file is JSON or CSV."""
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
                print("File not found.")
                self.data_type = None
        else:
            print("Data type detection is only applicable to local files.")

    def create_db(self, client):
        """Create or connect to a MongoDB collection."""
        try:
            db = client[self.db_name]
            self.collection = db[self.collection_name]
            print(f"Database '{self.db_name}' and collection '{self.collection_name}' are ready.")
        except Exception as e:
            print(f"Error creating database or collection: {e}")

    def load_data(self):
        """Load data from the source into the MongoDB collection."""
        if not self.collection:
            print("Collection not defined. Run 'create_db' first.")
            return

        if self.data_recon == "online":
            try:
                response = requests.get(self.source)
                response.raise_for_status()
                docs = self.collection.insert_many(response.json())
                print(f"{len(docs.inserted_ids)} documents added from online source.")
            except Exception as e:
                print(f"Error loading online data: {e}")
        elif self.data_recon == "local":
            if self.data_type == "JSON":
                try:
                    with open(self.source, 'r') as file:
                        docs = self.collection.insert_many(json.load(file))
                        print(f"{len(docs.inserted_ids)} documents added from JSON file.")
                except Exception as e:
                    print(f"Error loading JSON data: {e}")
            elif self.data_type == "CSV":
                try:
                    with open(self.source, 'r') as file:
                        reader = csv.DictReader(file)
                        docs = self.collection.insert_many(list(reader))
                        print(f"{len(docs.inserted_ids)} documents added from CSV file.")
                except Exception as e:
                    print(f"Error loading CSV data: {e}")
            else:
                print("Unsupported or unknown file format.")
        else:
            print("Source type not recognized. Cannot load data.")
