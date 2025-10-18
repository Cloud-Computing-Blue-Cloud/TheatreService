from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
import logging
from mysql.connector import Error

from models.mysql_connector import MySQLConnector


class TheatreDataService(MySQLConnector):
    """Service layer for Theatre data operations."""

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    def get_all_theatres(self) -> List[Dict[str, Any]]:
        try:
            with self.get_cursor(dictionary=True) as cursor:
                cursor.execute("SELECT * FROM theatres")
                results = cursor.fetchall()
                self.logger.info(f"Retrieved {len(results)} theatres from database")
                return results

        except Error as e:
            self.logger.error(f"Error retrieving theatres: {e}")
            raise
