from dagster import resource
import os
import paramiko
import stat


class SFTPResource:
    '''
    Establish a connection to a remote host via SSH.

    Provides methods for manipulating files/metadata on a remote host using
    SFTP. Built on Paramiko.

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
        return f'<SFTP Client::{self.username}@{self.hostname}::port={self.port}::connected={self.connected}>'

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

    def ls(self, path='.', include_dirs=True):
        ''' List the contents of a given directory '''
        if include_dirs:
            return self.sftp_client.listdir_attr(path)
        else:
            return [f for f in self.sftp_client.listdir_attr(path) if not stat.S_ISDIR(f.st_mode)]

    def get_file(self, from_path, to_path=None, mkdirs=True):
        ''' Fetch a single file from the server '''
        if to_path is None:
            to_path = from_path
        to_dir = os.path.split(to_path)[0]
        if mkdirs and not os.path.isdir(to_dir):
            os.makedirs(to_dir)
        self.sftp_client.get(from_path, to_path)

    def put_file(self, from_path, to_path=None, mkdirs=True):
        ''' Send a single file to the server '''
        if to_path is None:
            to_path = from_path
        to_dir = os.path.split(to_path)[0]
        if mkdirs and not os.path.isdir(to_dir):
            self._mkdirs(to_dir)
        self.sftp_client.put(from_path, to_path)

    def exists(self, path):
        ''' Boolean indicating whether a file is present on the host '''
        try:
            self.sftp_client.stat(path)
            return True
        except Exception as e:
            return False

    def tree(self, path):
        ''' List the contents of a given directory and all subdirectories '''
        files = []
        for file in self._recurse_directory(path):
            files.append(file)
        return files

    def _recurse_directory(self, current_directory):
        ''' Iterable, recurses directories within the remote host. Returns file attributes '''
        for attr in self.sftp_client.listdir_attr(current_directory):
            if not stat.S_ISDIR(attr.st_mode):
                yield attr
            elif stat.S_ISDIR(attr.st_mode):
                yield from self._recurse_directory(os.path.join(current_directory, attr.filename))

    def _mkdirs(self, path):
        ''' Emulates mkdir -p within the remote host '''
        directories = path.split('/')
        current_path = ''
        for dir in directories:
            current_path += f'/{dir}'
            try:
                self.sftp_client.chdir(current_path)
            except IOError as err:
                self.sftp_client.mkdir(current_path)


@resource
def sftp_resource(init_context):
    args = init_context.resource_config
    res = SFTPResource(**args)
    res.connect()
    return res
