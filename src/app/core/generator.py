from typing import Dict, Iterator, Any
from functools import lru_cache
from os import urandom
from base64 import b64encode

URANDOM_INT = 16
ENCODING = "utf-8"


class MessageGenerator:
    def __init__(self, schema: Dict[str, str], count: int = 100, unique: bool = True):

        self.schema = schema
        self.count = count
        self.unique = unique

    def _generate_unique_message(self) -> Dict[str, Any]:
        message = {}
        for field, data_type in self.schema.items():
            if data_type == "STRING":
                message[field] = b64encode(urandom(URANDOM_INT)).decode(encoding=ENCODING)
            elif data_type == "INTEGER":
                message[field] = int.from_bytes(urandom(URANDOM_INT))
            elif data_type == "FLOAT":
                message[field] = float(int.from_bytes(urandom(URANDOM_INT)))
        return message

    @lru_cache
    def _generate_identical_message(self) -> Dict[str, Any]:
        return self._generate_unique_message()

    def generate_once(self) -> Iterator[Dict[str, Any]]:
        for _ in range(self.count):
            if self.unique:
                yield self._generate_unique_message()
            else:
                yield self._generate_identical_message()

    # def generate_schedule(self, time_period: int, session_window: int) -> Iterator[Dict[str, Any]]:
    #     """
    #     Generate messages on a schedule.
    #
    #     :param time_period: Total time period in minutes during which all messages should be generated.
    #     :param session_window: Interval in seconds between each batch of messages.
    #     :return: An iterator yielding messages according to the schedule.
    #     """
    #     if time_period == 0 or session_window == 0:
    #         raise ValueError("time_period and session_window must be provided for scheduled intervals")
    #
    #     messages_per_batch = self.count // (time_period * 60 // session_window)
    #     start_time = datetime.now()
    #
    #     while datetime.now() < start_time + timedelta(seconds=time_period * 60):
    #         for _ in range(messages_per_batch):
    #             yield self._generate_message()
    #         time.sleep(session_window)

    def generate(self, schedule: str, time_period: int = 0, session_window: int = 0) -> Iterator[Dict[str, Any]]:

        if schedule == "once":
            return self.generate_once()
        # elif schedule == "schedule":
        #     return self.generate_schedule(time_period, session_window)
        else:
            raise Exception("Invalid interval value. Use 'once' or 'schedule'.")
