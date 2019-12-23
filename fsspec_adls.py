from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError

from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
from fsspec.spec import AbstractBufferedFile

import email.utils

LAZY_PROPERTY_ATTR_PREFIX = "_lazy_"


def lazy_property(fn):
    """Decorator that makes a property lazy-evaluated.
    On first access, lazy properties are computed and saved
    as instance attribute with the name `'_lazy_' + method_name`
    Any subsequent property access then returns the cached value."""
    attr_name = LAZY_PROPERTY_ATTR_PREFIX + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _lazy_property


class DataLakeFileSystem(AbstractFileSystem):
    tempdir = "/tmp"
    protocol = "adfs"
    _intrans = False

    def __init__(self, account_url, credential, file_system):
        self.account_url = account_url
        self.credential = credential
        self.file_system = file_system

    @lazy_property
    def filesystem(self):
        datalake = DataLakeServiceClient(
            account_url=self.account_url, credential=self.credential
        )
        return datalake.get_file_system_client(self.file_system)

    def isdir(self, path):
        # https://github.com/intake/filesystem_spec/blob/master/fsspec/spec.py#L530
        try:
            return super().isdir(path)
        except:
            return False

    def mkdir(self, path, create_parents=True, **kwargs):
        dir_client = self.filesystem.get_directory_client(path)
        dir_client.create_directory()

    def makedirs(self, path, exist_ok=False):
        dir_client = self.filesystem.get_directory_client(path)
        dir_client.create_directory()

    def rmdir(self, path):
        dir_client = self.filesystem.get_directory_client(path)
        dir_client.delete_directory()

    def checksum(self, path):
        # TODO check spec if this is a content checksum
        return self.info(path)["etag"]

    def _rm(self, path):
        """Delete a file"""
        file_client = self.filesystem.get_file_client(path)
        file_client.delete_file()

    def rm(self, path, recursive=False, maxdepth=None):
        """Delete files.
        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            NOT implemented always remove recursive
        maxdepth: int or None
            NOT implemented
        """
        if not isinstance(path, list):
            path = [path]
        for p in path:
            if self.isfile(p):
                self._rm(p)
            elif self.isdir(p):
                self._rmdir(p)

    def _rmdir(self, path):
        """Delete a file"""
        directory_client = self.filesystem.get_directory_client(path)
        directory_client.delete_directory()

    def mv(self, path1, path2, **kwargs):
        """ Move file from one location to another """
        if self.isfile(path1):
            file_client = self.filesystem.get_file_client(path1)
            file_client.rename_file(self.file_system + "/" + path2)
        elif self.isdir(path1):
            directory_client = self.filesystem.get_directory_client(path1)
            directory_client.rename_directory(self.file_system + "/" + path2)

    def ls(self, path, detail=False):
        infos = [
            {
                "name": info.name,
                "size": info.content_length,
                "type": "directory" if info.is_directory else "file",
                "permissions": info.permissions,
                "owner": info.owner,
                "group": info.group,
                "last_modified": email.utils.parsedate_to_datetime(
                    info.last_modified
                ).isoformat(),
                "etag": info.etag,
            }
            for info in self.filesystem.get_paths(path)
        ]
        if detail:
            return sorted(infos, key=lambda i: i["name"])
        else:
            return sorted(info["name"] for info in infos)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        return AzureDataLakeFile(self, path, mode=mode)


class AzureDataLakeFile(AbstractBufferedFile):
    """ File-like operations on Azure Data Lake Gen2"""

    def __init__(
        self,
        fs,
        path,
        mode="rb",
        block_size="default",
        autocommit=True,
        cache_type="readahead",
        cache_options=None,
        **kwargs,
    ):
        super().__init__(
            fs=fs,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_type=cache_type,
            cache_options=cache_options,
            **kwargs,
        )
        # especially ab is not supported
        if mode not in ["rb", "wb"]:
            raise NotImplementedError("File mode not supported")

    @lazy_property
    def file_client(self):
        if "w" in self.mode:
            file_client = self.fs.filesystem.create_file(self.path)
        else:
            file_client = self.fs.filesystem.get_file_client(self.path)
        return file_client

    def _fetch_range(self, start, end, **kwargs):
        return self.file_client.read_file(start, end)

    def _upload_chunk(self, final=False, **kwargs):
        data = self.buffer.getvalue()
        if len(data):
            self.file_client.append_data(data, self.loc - len(data), len(data))
        if final:
            self.file_client.flush_data(self.loc)
