from uuid import uuid4


class File:
    '''
    Represents a single file. Each file has a unique id and corresponding metadata.
    Args:
        _id         Unique id; defaults to uuid4()
        path        Pathlib Path representing location on system
        metadata    dictionary of metadata; defaults to {}
    '''

    def __init__(self, path, metadata={}, _id=None):
        self._id = _id or uuid4()
        self.path = path
        self.metadata = metadata

    def __hash__(self):
        return hash(self._id + self.path.as_posix())

    def __eq__(self, other):
        return hash(self) == hash(other)
