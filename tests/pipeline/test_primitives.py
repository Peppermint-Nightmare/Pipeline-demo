from pathlib import Path

import pytest

from src.pipeline.primitives import File


class TestFile:
    @pytest.mark.parametrize(
        'file1, file2, equal_flag',
        [
            (File(_id='1', path=Path('/file'), metadata={}), File(_id='1', path=Path('/file'), metadata={}), True),
            (
                File(_id='1', path=Path('file'), metadata={'a': 'b'}),
                File(_id='1', path=Path('file'), metadata={'a': 'b'}),
                True,
            ),
            (File(_id='1', path=Path('/file'), metadata={}), File(_id='2', path=Path('/file'), metadata={}), False),
            (File(_id='1', path=Path('/file'), metadata={}), File(_id='1', path=Path('/elif'), metadata={}), False),
            (
                File(_id='1', path=Path('/file'), metadata={'a': 'b'}),
                File(_id='1', path=Path('/file'), metadata={}),
                True,
            ),
        ],
    )
    def test_equality_between_files(self, file1, file2, equal_flag):
        assert (file1 == file2) == equal_flag
