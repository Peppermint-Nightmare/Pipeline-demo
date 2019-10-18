from src.pipeline.resources import SFTPResource
import pytest


@pytest.fixture
def files():
    return {
        'tmp': {},
        'a_dir': {'somefile.txt': "File content"},
        'listed_dir': {
            'a': 'a_contents',
            'b': 'b_contents',
            'subdir': {'sub_c': 'sub_c_contents', 'subsubdir': {'subsub_d': 'subsub_d_contents'}},
        },
    }


class TestSFTPResource:
    @staticmethod
    def test_open_close_connection(sftpserver, files):
        with sftpserver.serve_content(files):
            res = SFTPResource(hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw')
            assert res.connected is False
            res.connect()
            assert res.connected is True
            res.close()
            assert res.connected is False

    @staticmethod
    def test_put_file(sftpserver, files):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                assert not sftp_resource.exists('/tmp/somefile.txt')
                with open('/tmp/somefile.txt', 'w+') as f:
                    f.write('This is a file!')
                sftp_resource.put_file('/tmp/somefile.txt')
                assert sftp_resource.exists('/tmp/somefile.txt')

    @staticmethod
    def test_get_file(sftpserver, files):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:

                assert sftp_resource.exists('/a_dir/somefile.txt')
                sftp_resource.put_file('/tmp/somefile.txt')
                assert sftp_resource.exists('/tmp/somefile.txt')

    @staticmethod
    def test_exists(sftpserver, files):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                assert res.exists('a_dir/somefile.txt')

    @staticmethod
    def test_not_exists(sftpserver):
        with SFTPResource(
            hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
        ) as sftp_resource:
            assert not res.exists('a_dir/somefile.txt')

    @staticmethod
    def test_list_dir(sftpserver):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                # assert sftp_resource.ls(path='listed_dir') == ['a', 'b']
                for f in sftp_resource.ls('a_dir'):
                    print(f)

    @staticmethod
    def test_tree(sftpserver, files):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                print(sftp_resource.tree('listed_dir'))
