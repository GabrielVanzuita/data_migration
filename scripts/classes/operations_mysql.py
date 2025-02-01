from bson.objectid import ObjectId
import logging
from loguru import logger

# Logging configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')
logger.add(r"............", format="{time} {level} {message}", level="DEBUG")


class mySQLoperator:
    def __init__(self, operations_mongo, db_name, connector, table_name, columns):
        """
        Initializes the handling of data and database.

        :param operations_mongo: Instance of the mongoOperator class
        :param db_name: MySQL database name
        :param table_name: MySQL table name
        :param connector: MySQL connection
        :param columns: Definition of MySQL table columns
        """
        self.operations_mongo = operations_mongo  # Instance of the mongoOperator class
        self.connector = connector  # MySQL connection
        self.db_name = db_name  # Database name
        self.table_name = table_name  # Table name
        self.columns = columns  # Columns definition
        self.data_print = operations_mongo.data_print  # Data to migrate from MongoDB
    
    def create_database(self):
        """
        Creates the database if it does not exist.
        """
        cursor = self.connector.cursor()
        try: 
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{self.db_name}`")
            self.connector.commit()  # Ensure persistence
            print(f"Database '{self.db_name}' created or already exists.")
        except Exception as e:
            print(f"Error creating the database: {e}")
        finally:
            cursor.close()  # Close the cursor after execution

    def create_table(self):
        """
        Creates the table if it does not exist.
        This method only focuses on creating the table, without defining columns.
        """
        cursor = self.connector.cursor()
        try:
            cursor.execute(f"USE `{self.db_name}`")  # Select the database
            create_table_query = f"CREATE TABLE IF NOT EXISTS `{self.table_name}` ()"
            cursor.execute(create_table_query)
            self.connector.commit()  # Ensure persistence
            print(f"Table '{self.table_name}' created or already exists.")
        except Exception as e:
            print(f"Error creating the table: {e}")
        finally:
            cursor.close()  # Close the cursor after execution

    def create_columns(self):
        """
        Defines the columns of the table based on the provided columns parameter.
        This method is responsible only for creating the columns.
        """
        cursor = self.connector.cursor()
        try:
            cursor.execute(f"USE `{self.db_name}`")  # Select the database
            columns_def = ", ".join(f"`{col['name']}` {col['type']}" for col in self.columns)
            alter_table_query = f"ALTER TABLE `{self.table_name}` ADD ({columns_def})"
            cursor.execute(alter_table_query)
            self.connector.commit()  # Ensure persistence
            print(f"Columns for table '{self.table_name}' created or already exist.")
        except Exception as e:
            print(f"Error creating the columns: {e}")
        finally:
            cursor.close()  # Close the cursor after execution

    def migrate_data(self):
        """
        Migrates data from MongoDB to MySQL, preserving the _id as VARCHAR(24).
        This method only inserts the data, assuming the table and columns are already created.
        """
        logger.info("Starting data migration.")
    
        if not self.data_print:
            logger.warning("No data to migrate.")
            return

        cursor = self.connector.cursor()
        try:
            cursor.execute(f"USE `{self.db_name}`")  # Select the database

            for document in self.data_print:
                try:
                    if '_id' in document and isinstance(document['_id'], ObjectId):
                        mongo_id = document['_id']
                        hex_value = mongo_id.binary.hex()[:24]

                        if len(mongo_id.binary.hex()) > 24:
                            logger.warning(f"Truncation detected for _id: {mongo_id} (truncated to {hex_value})")

                        document['_id'] = hex_value

                    keys = ", ".join(f"`{key}`" for key in document.keys())
                    placeholders = ", ".join(["%s"] * len(document))
                    values = tuple(document.values())
                    sql = f"INSERT INTO `{self.table_name}` ({keys}) VALUES ({placeholders})"

                    # Detailed log for SQL execution
                    logger.debug("--- SQL EXECUTION START ---")
                    logger.debug(f"Executing SQL: {sql}")
                    logger.debug(f"With values: {values}")
                    logger.debug("--- SQL EXECUTION END ---")

                    cursor.execute(sql, values)

                except Exception as doc_error:
                    logger.error(f"Error migrating document {document}: {doc_error}")

            self.connector.commit()
            logger.info("Migration completed successfully.")
        except Exception as e:
            logger.error(f"Error during data migration: {e}")
            self.connector.rollback()
        finally:
            cursor.close()  # Close the cursor after execution
            logger.info("Cursor connection closed.")
    
    # Getter and Setter for operations_mongo
    @property
    def operations_mongo(self):
        return self._operations_mongo

    @operations_mongo.setter
    def operations_mongo(self, value):
        self._operations_mongo = value

    # Getter and Setter for connector
    @property
    def connector(self):
        return self._connector

    @connector.setter
    def connector(self, value):
        self._connector = value
