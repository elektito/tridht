import random

class InfohashDb:
    def __init__(self):
        self.infohashes = set()

    def add_infohash(self, ih):
        self.infohashes.add(ih)

    def size(self):
        return len(self.infohashes)

    def serialize(self):
        return {
            'infohashes': [
                ih.hex() for ih in self.infohashes
            ],
        }

    @classmethod
    def deserialize(cls, state):
        db = cls()
        if state:
            db.infohashes = {
                bytes.fromhex(ih)
                for ih in state['infohashes']
            }
        return db
