import mysql.connector
from mysql.connector import Error, pooling
from typing import Optional, Any, Dict
import logging
from contextlib import contextmanager
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class MySQLConfig(BaseSettings):
    mysql_host: str = Field(..., description="MySQL server IP address")
    mysql_port: int = Field(default=3306, description="MySQL port")
    mysql_user: str = Field(..., description="MySQL username")
    mysql_password: SecretStr = Field(..., description="MySQL password")
    mysql_database: str = Field(..., description="Database name")

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


class MySQLConnector:
    def __init__(self):
        self.config = MySQLConfig() # type: ignore
        self.logger = logging.getLogger(__name__)

    def get_connection(self):
        """Get a new database connection."""
        try:
            connection = mysql.connector.connect(
                host=self.config.mysql_host,
                port=self.config.mysql_port,
                user=self.config.mysql_user,
                password=self.config.mysql_password.get_secret_value(),
                database=self.config.mysql_database,
            )
            return connection
        except Error as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise

    @contextmanager
    def get_cursor(self, dictionary: bool = False, buffered: bool = False):
        connection = self.get_connection()
        cursor = None
        try:
            cursor = connection.cursor(dictionary=dictionary, buffered=buffered)
            yield cursor
            connection.commit()
        except Error as e:
            if connection:
                connection.rollback()
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute SELECT query and return results."""
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()

    def execute_update(self, query: str, params: Optional[tuple] = None):
        """Execute INSERT/UPDATE/DELETE and return affected rows."""
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.rowcount

    def test_connection(self) -> bool:
        """
        Test the database connection.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with self.get_cursor(dictionary=False) as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                self.logger.info("Database connection test successful")
                return result[0] == 1
        except Error as e:
            self.logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    connector = MySQLConnector()

    connector.test_connection()
