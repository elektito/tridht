import random

class InfohashDb:
    def __init__(self):
        self.infohashes = set()
        self.metadata = {}

    def add_infohash(self, ih):
        self.infohashes.add(ih)

    def size(self):
        return len(self.infohashes)

    def store_metadata(self, ih, metadata):
        self.metadata[ih] = metadata

    def update_ih_status(self, ih, status):
        #self.infohashes[ih].next_check_time =
        #self.infohashes[ih].score +=
        print('UPDATE IH STATUS: YYYYYYYYYYY')

    def serialize(self):
        return {
            'infohashes': [
                ih.hex() for ih in self.infohashes
            ],
            'metadata': {
                ih.hex(): metadata.hex()
                for ih, metadata in self.metadata.items()
            },
        }

    @classmethod
    def deserialize(cls, state):
        db = cls()
        if state:
            db.infohashes = {
                bytes.fromhex(ih)
                for ih in state['infohashes']
            }
            db.metadata = {
                bytes.fromhex(ih): bytes.fromhex(metadata)
                for ih, metadata in state.get('metadata', {}).items()
            }
        return db
