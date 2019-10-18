from dagster import resource
import os
import paramiko
import stat


class SFTPResource:
    '''
    Establish a connection to a remote host via SSH.

    Provides methods for manipulating files/metadata on a remote host using
    SFTP. Built with Paramiko.

    Parameters
    ----------
    hostname : str
        Hostname for the resource
    port : int
        Port for the resource
    username : str
        Username to establish connection
    private_key : str
        The private key to use for connection, in raw text form.
    '''

    def __init__(self, hostname, port, username, password):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __str__(self):
        return f'<SFTP Client::{self.username}@{self.hostname}:{self.port}::connected={self.connected}>'

    @property
    def connected(self):
        ''' Attribute indicating whether the connection is still alive '''
        try:
            self.sftp_client.listdir('/')
            return True
        except Exception as e:
            return False

    def connect(self):
        ''' Attempt to establish a connection to the provided hostname '''
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=self.hostname, port=self.port, username=self.username, password=self.password)
        self.ssh_client, self.sftp_client = client, client.open_sftp()

    def close(self):
        ''' Kill the SFTP & SSH clients '''
        self.sftp_client.close()
        self.ssh_client.close()

    def ls(self, path='.'):
        ''' List the contents of a given directory '''
        return self.sftp_client.listdir_attr(path)

    def ls_iter(self, path):
        ''' Iterable of the contents of a given directory '''
        for f in self.sftp_client.listdir_iter_attr(path):
            yield f

    def tree(self, path, max_depth=None):
        ''' List the contents of a given directory and all subdirectories '''
        files = []
        for f in self._recurse_directory(path, max_iterations=max_depth):
            files.append(f)
        return files

    def tree_iter(self, path, max_depth=None):
        ''' Iterable of the contents of a given directory and all subdirectories '''
        for f in self._recurse_directory(path, max_iterations=max_depth):
            yield f

    def _recurse_directory(self, current_directory, max_iterations=None, iteration_count=0):
        ''' Iterable, recurses directories within the remote host. Returns file attributes '''
        for attr in self.sftp_client.listdir_iter(current_directory):
            if not stat.S_ISDIR(attr.st_mode):
                print(f'file: {attr.filename}')
                yield attr
            else:
                print(f'Yielding from directory: {attr.filename}')
                yield from self._recurse_directory(os.path.join(current_directory, attr.filename))

    def get_file(self, from_path, to_path=None, mkdirs=True):
        ''' Fetch a single file from the server '''
        if to_path is None:
            to_path = from_path
        self.sftp_client.get(from_path, to_path)

    def put_file(self, from_path, to_path=None, mkdirs=True):
        ''' Send a single file to the server '''
        if to_path is None:
            to_path = from_path
        print(f'Uploading from {from_path} to {to_path}')
        self.sftp_client.put(from_path, to_path)

    def exists(self, path):
        ''' Boolean indicating whether a file is present on the host '''
        try:
            self.sftp_client.stat(path)
            return True
        except Exception as e:
            return False

    def _mkdirs(self, path):
        ''' Ensure that the directory at path and all its children exist '''
        raise NotImplementedError


@resource
def sftp_resource(init_context):
    args = init_context.resource_config
    res = SFTPResource(**args)
    res.connect()
    return res
