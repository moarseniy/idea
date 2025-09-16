from abc import ABC, abstractmethod
class Singleton(ABC):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Singleton, cls).__new__(cls)
            cls.instance._setup()
        return cls.instance
    @abstractmethod
    def _setup(self):
      pass