import mysql.connector
from mysql.connector import Error

class MySQLCon:
    def __init__(self, host, username, password):
        """
        Initializes the MySQL connection with provided credentials.
        :param host: MySQL server host
        :param username: MySQL username
        :param password: MySQL password
        """
        self.host = host
        self.username = username
        self.password = password
        self.connection = None  # Initializes the connection as None, will be set after connection
        self.cursor = None  # Initializes the cursor as None, will be set after connection

    def connect(self):
        """
        Establishes the connection to MySQL without accessing a specific database.
        :returns: MySQL connection object if successful, None if failed.
        """
        try:
            # Establishes the connection to the MySQL server
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.username,
                password=self.password
            )
            if self.connection.is_connected():  # Checks if the connection was successfully established
                # Creates a cursor from the connection
                self.cursor = self.connection.cursor()
                print("MySQL connection established successfully.")
                return self.connection
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return None

    def use_database(self, db):
        """
        Selects a specific database after the connection is established.
        :param db: Name of the database to use
        :returns: None
        """
        if not self.connection or not self.connection.is_connected():
            print("No active connection.")
            return None
        try:
            # Attempts to use the specified database
            self.cursor.execute(f"USE {db}")
            print(f"Database '{db}' selected.")
        except Error as e:
            print(f"Error accessing the database '{db}': {e}")
            return None

    def close_con(self):
        """
        Closes the MySQL connection and cursor.
        :returns: None
        """
        try:
            if self.cursor:
                self.cursor.close()  # Closes the cursor if it exists
            if self.connection:
                self.connection.close()  # Closes the connection if it exists
            print("Connection closed successfully.")
        except Exception as e:
            print(f"The connection was not closed. Error: {e}")
    
    # Getter and Setter for host
    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        self._host = value

    # Getter and Setter for username
    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, value):
        self._username = value

    # Getter and Setter for password
    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, value):
        self._password = value

    # Getter and Setter for connection
    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, value):
        self._connection = value

    # Getter and Setter for cursor
    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, value):
        self._cursor = value
