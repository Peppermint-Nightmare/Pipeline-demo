from src.pipeline.resources import SFTPResource
import os
import pytest


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
            print(res)

    @staticmethod
    @pytest.mark.parametrize(
        'file_to_put',
        [
            # {'from_path': '/a/b/c/somefile.txt', 'to_path': '/tmp/somefile.txt', 'mkdirs': False},
            {'from_path': 'root_located_file', 'to_path': '/1_file/2_file', 'mkdirs': True},
            # {'from_path': '/i/am/a/path/to/a/file', 'to_path': '/red_file/blue_file', 'mkdirs': True},
            # {'from_path': '/root_file.dat', 'to_path': '/root_file.dat', 'mkdirs': False},
        ],
    )
    def test_put_file(sftpserver, files, tmp_path, file_to_put):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                file_to_put['from_path'] = os.path.join(tmp_path, file_to_put['from_path'].strip('/'))
                assert not sftp_resource.exists(file_to_put['to_path'])

                with open(file_to_put['from_path'], 'w+') as f:
                    f.write('This is a file!')
                sftp_resource.put_file(**file_to_put)

                assert sftp_resource.exists(file_to_put['to_path'])

    @staticmethod
    @pytest.mark.parametrize(
        'file_to_get',
        [
            {'from_path': '/a_dir/somefile.txt', 'to_path': '/a_dir/somefile.txt', 'mkdirs': True},
            {'from_path': '/listed_dir/subdir/sub_c', 'to_path': '/1_file/2_file', 'mkdirs': True},
            {'from_path': '/listed_dir/subdir/subsubdir/subsub_d', 'to_path': '/red_file/blue_file', 'mkdirs': True},
            {'from_path': '/root_file.dat', 'to_path': '/root_file.dat', 'mkdirs': False},
        ],
    )
    def test_get_file(sftpserver, files, tmp_path, file_to_get):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                if 'to_path' in file_to_get.keys():
                    path_to_check = os.path.join(tmp_path, file_to_get['to_path'].strip('/'))
                    file_to_get['to_path'] = path_to_check
                else:
                    path_to_check = os.path.join(tmp_path, file_to_get['from_path'].strip('/'))
                    print(path_to_check)

                assert not os.path.isfile(path_to_check)
                sftp_resource.get_file(**file_to_get)
                assert os.path.isfile(path_to_check)

    @staticmethod
    @pytest.mark.parametrize(
        'test_file',
        [
            '/root_file.dat',
            '/a_dir/somefile.txt',
            'listed_dir/a',
            'listed_dir/subdir/sub_c',
            '/listed_dir/subdir/subsubdir/subsub_d',
        ],
    )
    def test_exists(sftpserver, files, test_file):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                assert sftp_resource.exists(test_file)

    @staticmethod
    @pytest.mark.parametrize(
        'test_file',
        [
            'root_file.data',
            '/b_dir/somefile.txt',
            'listed_dir/c',
            'listed_dir/subdir/b',
            '/listed_dir/subdir/subsubdir/sub_c',
        ],
    )
    def test_not_exists(sftpserver, files, test_file):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                assert not sftp_resource.exists(test_file)

    @staticmethod
    @pytest.mark.parametrize(
        'ls_params, expected_output',
        [
            ({'path': 'listed_dir', 'include_dirs': False}, ['a', 'b']),
            ({'path': 'listed_dir', 'include_dirs': True}, ['a', 'b', 'subdir', 'other_subdir']),
            ({'path': '/', 'include_dirs': False}, ['root_file.dat']),
            ({'path': '/', 'include_dirs': True}, ['root_file.dat', 'tmp', 'a_dir', 'listed_dir']),
        ],
    )
    def test_list(sftpserver, files, ls_params, expected_output):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                assert [x.filename for x in sftp_resource.ls(**ls_params)] == expected_output

    @staticmethod
    @pytest.mark.parametrize(
        'tree_params, expected_output',
        [
            ({'path': 'listed_dir'}, ['a', 'b', 'sub_c', 'subsub_d', 'i', 'a', 'of']),
            ({'path': 'listed_dir/subdir'}, ['sub_c', 'subsub_d']),
            ({'path': 'listed_dir/other_subdir'}, ['i', 'a', 'of']),
        ],
    )
    def test_tree(sftpserver, files, tree_params, expected_output):
        with sftpserver.serve_content(files):
            with SFTPResource(
                hostname=sftpserver.host, port=sftpserver.port, username='user', password='pw'
            ) as sftp_resource:
                assert [x.filename for x in sftp_resource.tree(**tree_params)] == expected_output
