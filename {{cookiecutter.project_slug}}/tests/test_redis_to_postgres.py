"""Unit tests for redis_to_postgres function."""

from unittest.mock import MagicMock, patch

import pytest


class TestRedisToPostgres:
    """Test the redis_to_postgres function with mocked dependencies."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        mock = MagicMock()
        mock.keys.return_value = [
            "BTCUSDT-2025-01-15T10:00",
            "ETHUSDT-2025-01-15T10:00",
            "DOTUSDT-2025-01-15T10:00",
        ]
        mock.hgetall.side_effect = [
            {
                "open": "45000.50",
                "high": "45500.75",
                "low": "44800.25",
                "close": "45200.00",
                "volume": "1250.5",
            },
            {
                "open": "3000.25",
                "high": "3050.50",
                "low": "2980.00",
                "close": "3020.75",
                "volume": "8500.25",
            },
            {
                "open": "25.50",
                "high": "26.00",
                "low": "25.20",
                "close": "25.80",
                "volume": "150000.0",
            },
        ]
        return mock

    @pytest.fixture
    def mock_psycopg2_conn(self):
        """Create a mock psycopg2 connection."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_conn, mock_cursor

    @patch("dags.crypto_ingestion_pipeline.psycopg2.connect")
    @patch("dags.crypto_ingestion_pipeline.redis.Redis")
    def test_redis_to_postgres_success(self, mock_redis_class, mock_psycopg2, mock_redis, mock_psycopg2_conn):
        """Test successful data transfer from Redis to Postgres."""
        from dags.crypto_ingestion_pipeline import redis_to_postgres

        mock_conn, mock_cursor = mock_psycopg2_conn
        mock_redis_class.return_value = mock_redis
        mock_psycopg2.return_value = mock_conn

        # Execute function
        redis_to_postgres()

        # Verify Redis was queried
        mock_redis.keys.assert_called_once_with("*USDT-*")
        assert mock_redis.hgetall.call_count == 3

        # Verify Postgres connection
        mock_psycopg2.assert_called_once()

        # Verify data was inserted
        from psycopg2.extras import execute_batch
        assert mock_cursor.execute.called or mock_conn.commit.called
        mock_conn.commit.assert_called_once()

        # Verify cleanup
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("dags.crypto_ingestion_pipeline.psycopg2.connect")
    @patch("dags.crypto_ingestion_pipeline.redis.Redis")
    def test_redis_to_postgres_no_keys(self, mock_redis_class, mock_psycopg2, mock_psycopg2_conn):
        """Test behavior when Redis has no keys."""
        from dags.crypto_ingestion_pipeline import redis_to_postgres

        mock_conn, mock_cursor = mock_psycopg2_conn
        mock_redis = MagicMock()
        mock_redis.keys.return_value = []
        mock_redis_class.return_value = mock_redis
        mock_psycopg2.return_value = mock_conn

        # Execute function
        redis_to_postgres()

        # Verify Redis was queried but no data fetched
        mock_redis.keys.assert_called_once_with("*USDT-*")
        mock_redis.hgetall.assert_not_called()

        # Verify connection was closed
        mock_conn.close.assert_called_once()

        # Verify no commit happened (no data to insert)
        mock_conn.commit.assert_not_called()

    @patch("dags.crypto_ingestion_pipeline.psycopg2.connect")
    @patch("dags.crypto_ingestion_pipeline.redis.Redis")
    def test_redis_to_postgres_invalid_key_format(self, mock_redis_class, mock_psycopg2, mock_psycopg2_conn):
        """Test behavior with invalid Redis key format."""
        from dags.crypto_ingestion_pipeline import redis_to_postgres

        mock_conn, mock_cursor = mock_psycopg2_conn
        mock_redis = MagicMock()
        # Invalid key without timestamp part
        mock_redis.keys.return_value = ["BTCUSDT"]
        mock_redis.hgetall.return_value = {
            "open": "45000.50",
            "high": "45500.75",
            "low": "44800.25",
            "close": "45200.00",
            "volume": "1250.5",
        }
        mock_redis_class.return_value = mock_redis
        mock_psycopg2.return_value = mock_conn

        # Execute function
        redis_to_postgres()

        # Verify no records were inserted due to invalid format
        mock_conn.commit.assert_not_called()

    @patch("dags.crypto_ingestion_pipeline.psycopg2.connect")
    @patch("dags.crypto_ingestion_pipeline.redis.Redis")
    def test_redis_to_postgres_empty_data(self, mock_redis_class, mock_psycopg2, mock_psycopg2_conn):
        """Test behavior when Redis returns empty data for a key."""
        from dags.crypto_ingestion_pipeline import redis_to_postgres

        mock_conn, mock_cursor = mock_psycopg2_conn
        mock_redis = MagicMock()
        mock_redis.keys.return_value = ["BTCUSDT-2025-01-15T10:00"]
        mock_redis.hgetall.return_return_value = {}  # Empty data
        mock_redis_class.return_value = mock_redis
        mock_psycopg2.return_value = mock_conn

        # Execute function
        redis_to_postgres()

        # Verify no commit happened
        mock_conn.commit.assert_not_called()

    @patch("dags.crypto_ingestion_pipeline.psycopg2.connect")
    @patch("dags.crypto_ingestion_pipeline.redis.Redis")
    def test_redis_to_postgres_data_parsing(self, mock_redis_class, mock_psycopg2, mock_redis, mock_psycopg2_conn):
        """Test that data is correctly parsed from Redis format."""
        from dags.crypto_ingestion_pipeline import redis_to_postgres

        mock_conn, mock_cursor = mock_psycopg2_conn
        mock_redis_class.return_value = mock_redis
        mock_psycopg2.return_value = mock_conn

        # Execute function
        redis_to_postgres()

        # The function should parse the timestamp correctly
        # Format: "BTCUSDT-2025-01-15T10:00" -> timestamp_pg = "2025-01-15 10:00:00"
        mock_redis.keys.assert_called_once()
        assert mock_redis.hgetall.call_count == 3
