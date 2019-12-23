import pytest
from fsspec_adls import DataLakeFileSystem
from azure.core.exceptions import ResourceNotFoundError


@pytest.fixture
def dfs(account_name, account_key, filesystem_name):
    account_url = "https://{}.dfs.core.windows.net/".format(account_name)
    return DataLakeFileSystem(account_url, account_key, filesystem_name)


def test_ls(filesystem, dfs):
    assert dfs.ls("root") == [
        "root/a",
        "root/a/file.txt",
        "root/b",
        "root/b/file.txt",
        "root/c",
        "root/c/file.txt",
        "root/empty_dir",
    ]

    assert dfs.ls("root/empty_dir") == []
    assert dfs.ls("root/empty_dir", detail=True) == []

    assert dfs.ls("root/a/file.txt") == ["root/a/file.txt"]

    res = dfs.ls("root/a/file.txt", detail=True)
    assert len(res) == 1
    expected_minimal = {
        "name": "root/a/file.txt",
        "size": 9,
        "type": "file",
        "permissions": "rw-r-----",
        "owner": "$superuser",
        "group": "$superuser",
    }
    for k, v in expected_minimal.items():
        assert res[0][k] == v

    assert "last_modified" in res[0]
    assert "etag" in res[0]

    # with pytest.raises(StorageErrorException):
    with pytest.raises(Exception):
        dfs.ls("root/noexistent")


def test_isdir(filesystem, dfs):
    assert dfs.isdir("root/empty_dir")
    assert not dfs.isdir("root/a/file.txt")

    assert not dfs.isdir("root/noexistent")


def test_isfile(filesystem, dfs):
    assert dfs.isfile("root/a/file.txt")
    assert not dfs.isfile("root/empty_dir")

    # with pytest.raises(StorageErrorException):
    assert not dfs.isfile("root/noexistent")


def test_info(filesystem, dfs):
    assert dfs.info("root/a/file.txt") == dfs.ls("root/a/file.txt", detail=True)[0]


def test_mkdir(filesystem_volatile, dfs):
    dirname = "root/volatile/testdir"
    dfs.mkdir(dirname)
    assert dfs.isdir(dirname)
    dfs.rm(dirname)
    assert not dfs.exists(dirname)


def test_mkdirs(filesystem_volatile, dfs):
    dirname = "root/volatile/testdir/nested"
    dfs.mkdirs(dirname)
    assert dfs.isdir(dirname)
    dfs.rm(dirname)
    assert not dfs.exists(dirname)


def test_rm(filesystem_volatile, dfs):
    dirname = "root/volatile/testdir/nested"
    dfs.mkdirs(dirname)
    dfs.rm(dirname)
    assert not dfs.exists(dirname)

    # Test nested deletion
    dfs.mkdirs(dirname)
    with dfs.open("root/volatile/testdir/file.txt", "wb") as f:
        f.write(b"data")
    dfs.rm("root/volatile/testdir/file.txt")
    assert not dfs.exists("root/volatile/testdir/file.txt")

    dfs.rm("root/volatile/testdir")
    assert not dfs.exists(dirname)


def test_rmdir(filesystem_volatile, dfs):
    dirname = "root/volatile/testdir/nested"
    dfs.mkdirs(dirname)
    dfs.rmdir(dirname)
    assert not dfs.exists(dirname)


def test_mv(filesystem_volatile, dfs):
    with dfs.open("root/volatile/testdir/file.txt", "wb") as f:
        f.write(b"data")
    dfs.mv("root/volatile/testdir/file.txt", "root/volatile/testdir/file_new.txt")
    assert dfs.exists("root/volatile/testdir/file_new.txt")

    dfs.mv("root/volatile/testdir", "root/volatile/testdir_new")
    assert dfs.exists("root/volatile/testdir_new")
    assert dfs.exists("root/volatile/testdir_new/file_new.txt")

    # TODO: should raise?
    # dfs.mv("root/noexistent", "root/volatile/testdir_noexistent")


def test_open(filesystem_volatile, dfs):
    data = b"data"
    with dfs.open("root/volatile/testdir/file.txt", "wb") as f:
        f.write(data)

    with dfs.open("root/volatile/testdir/file.txt", "rb") as f:
        resp = f.read()
    assert resp == data
