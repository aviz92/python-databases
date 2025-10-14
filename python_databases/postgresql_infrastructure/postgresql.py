import os
from types import TracebackType
from typing import Any

import psycopg2
from custom_python_logger import get_logger
from psycopg2 import extras


class PostgreSQL:
    def __init__(
        self,
        name: str | None = None,
        host: str | None = None,
        port: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self.logger = get_logger(self.__class__.__name__)

        self.name = name or os.getenv("POSTGRESQL_NAME")
        self.host = host or os.getenv("POSTGRESQL_HOST")
        self.port = port or os.getenv("POSTGRESQL_PORT")
        self.username = username or os.getenv("POSTGRESQL_USERNAME")
        self.password = password or os.getenv("POSTGRESQL_PASSWORD")

        self._client = None
        self._cursor = None

        self.create_connection()

    @property
    def client(self) -> psycopg2.extensions.connection:
        if not self._client or self._client.closed:
            self.logger.warning("Re-establishing PostgreSQL connection")
            self.create_connection()
        return self._client

    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        if not self._cursor or self._cursor.closed:
            self.logger.warning("Re-creating PostgreSQL cursor")
            self.create_connection()
        return self._cursor

    # @retry(stop_max_attempt_number=3, wait_fixed=180000)
    def create_connection(self) -> None:
        self.logger.info(f"Create connection to {self.host}:{self.port}")

        self._client = psycopg2.connect(
            database=self.name,
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
        )
        self._cursor = self._client.cursor(cursor_factory=extras.DictCursor)

    def execute_query(self, query: str, params: tuple | dict[str, Any] | None = None) -> None:
        self.logger.debug(f"Fetch rows from DB by query {query} with params {params}")
        try:
            self.cursor.execute(query, params)
        except psycopg2.ProgrammingError as e:
            raise psycopg2.ProgrammingError(f"Error executing query: {query}") from e
        self.logger.debug("Finished to fetch rows by query")

    def fetch_all(self, query: str, params: tuple | dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.execute_query(query, params)
        try:
            results = self.cursor.fetchall()
        except psycopg2.ProgrammingError as e:
            raise psycopg2.ProgrammingError(f"Error fetching rows: {query}") from e
        self.logger.debug(f"Fetched {len(results)} rows")
        return results

    def close(self) -> None:
        self.cursor.close()
        self.client.close()

    def __enter__(self) -> "PostgreSQL":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
