import pytest


@pytest.fixture
def files():
    return {
        'root_file.dat': 'root file contents',
        'tmp': {},
        'a_dir': {'somefile.txt': "File content"},
        'listed_dir': {
            'a': 'a_contents',
            'b': 'b_contents',
            'subdir': {'sub_c': 'sub_c_contents', 'subsubdir': {'subsub_d': 'subsub_d_contents'}},
            'other_subdir': {'i': 'am', 'a': 'set', 'of': 'files'},
        },
    }
