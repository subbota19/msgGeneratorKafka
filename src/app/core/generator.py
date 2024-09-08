from base64 import b64encode
from datetime import datetime
from functools import lru_cache
from os import urandom
from typing import (
    Any,
    Dict,
    Iterator,
)


URANDOM_INT = 4
ENCODING = "utf-8"

DICT_TYPER = {
    "STRING": lambda: b64encode(urandom(URANDOM_INT)).decode(
        encoding=ENCODING
    ),
    "INTEGER": lambda: int.from_bytes(urandom(URANDOM_INT)),
    "FLOAT": lambda: float(int.from_bytes(urandom(URANDOM_INT))),
    "DATETIME": lambda: datetime.now().isoformat(),
}


class MessageGenerator:
    def __init__(
        self,
        schema: Dict[str, str],
        count: int = 100,
        unique: bool = True,
    ):
        self.schema = schema
        self.count = count
        self.unique = unique

    def _generate_unique_message(
        self,
    ) -> Dict[str, Any]:
        message = {}
        for field, data_type in self.schema.items():
            func_typer = DICT_TYPER.get(data_type)

            if func_typer:
                message[field] = func_typer()

        return message

    @lru_cache
    def _generate_identical_message(
        self,
    ) -> Dict[str, Any]:
        return self._generate_unique_message()

    def generate_once(
        self,
    ) -> Iterator[Dict[str, Any]]:
        for _ in range(self.count):
            if self.unique:
                yield self._generate_unique_message()
            else:
                yield self._generate_identical_message()

    def generate(
        self,
        schedule: str,
        time_period: int = 0,
        session_window: int = 0,
    ) -> Iterator[Dict[str, Any]]:
        if schedule == "once":
            return self.generate_once()
        # elif schedule == "schedule":
        #     return self.generate_schedule(time_period, session_window)
        else:
            raise Exception(
                "Invalid interval value. Use 'once' or 'schedule'."
            )
