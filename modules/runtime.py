import argparse
import logging
import os
from datetime import datetime
from typing import Any, Optional

import pymysql
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TIMEOUT = 20


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def load_environment() -> None:
    load_dotenv()


def get_target_year(cli_year: Optional[int] = None) -> int:
    if cli_year is not None:
        return cli_year
    env_value = os.getenv("TARGET_YEAR")
    if env_value:
        try:
            return int(env_value)
        except ValueError as exc:
            raise ValueError(f"Invalid TARGET_YEAR value: {env_value}") from exc
    return datetime.utcnow().year


def parse_year_arg(default: Optional[int] = None) -> int:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--year", type=int, default=default)
    args = parser.parse_args()
    return get_target_year(args.year)


def get_db_connection() -> pymysql.connections.Connection:
    load_environment()
    password = os.getenv("DB_PASSWD") or os.getenv("DB_PASSWORD")
    port = int(os.getenv("DB_PORT", "3306"))
    required_vars = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_NAME": os.getenv("DB_NAME"),
    }
    missing = [name for name, value in required_vars.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required database env vars: {', '.join(missing)}")
    if not password:
        raise RuntimeError("Missing database password. Set DB_PASSWD or DB_PASSWORD.")
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=port,
        user=os.getenv("DB_USER"),
        passwd=password,
        db=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
        use_unicode=True,
        charset=os.getenv("DB_CHARSET", "utf8mb4"),
        autocommit=False,
    )


def get_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "racingmike-motogp-import/2.0",
            "Accept": "application/json, text/plain, */*",
        }
    )
    return session


def request_json(session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT) -> Any:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def dict_get(data: Any, *keys: str, default: Any = None) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current
