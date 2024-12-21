from asyncio import TimeoutError, create_task, gather, sleep, wait_for
from time import time

from app.abstracts.producer_manger import AbstractProducerManager
from app.core.generator import (
    MessageGenerator,
)


class AsyncioGenerator:
    def __init__(
        self,
        producer: AbstractProducerManager,
        message_generator: MessageGenerator,
        parallelism: int = 1,
        time_period=None,
        session_window=None,
        topic_name: str = None,
    ):
        self.producer = producer
        self.message_generator = message_generator
        self.parallelism = parallelism
        self.time_period = time_period
        self.session_window = session_window
        self.topic_name = topic_name

    async def push_event(self):
        # TODO: implement async publish by using aiokafka
        for msg in self.message_generator.generate():
            self.producer.publish_msg(
                topic=self.topic_name,
                value=msg,
            )

    async def process(self, client_id):
        total_windows = 1
        if self.time_period is not None and self.session_window is not None:
            total_windows = self.time_period // self.session_window

        for window_index in range(total_windows):
            print(f"Client {client_id} - Starting window {window_index}")
            try:
                start_time = time()
                message_task = create_task(self.push_event())
                await wait_for(message_task, timeout=self.session_window)
                remaining_time = self.session_window - (time() - start_time)
                if remaining_time > 0:
                    await sleep(remaining_time)

            except TimeoutError:
                print("Task timed out and was canceled")
            print(f"Client {client_id} - Completed window {window_index}")

    async def generate(self):
        tasks = [create_task(self.process(_)) for _ in range(self.parallelism)]

        await gather(*tasks)
