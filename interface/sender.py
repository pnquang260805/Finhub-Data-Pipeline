from abc import ABC, abstractmethod


class Sender(ABC):
    @abstractmethod
    def send_message(self, message: object, *args, **kwargs):
        pass
