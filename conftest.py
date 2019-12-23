import pytest
import os
from azure.storage.filedatalake import DataLakeServiceClient

@pytest.fixture(scope="session")
def account_name():
    return os.getenv("STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("STORAGE_ACCOUNT_KEY")

@pytest.fixture(scope="session")
def account_key():
    return os.getenv("STORAGE_ACCOUNT_KEY")

@pytest.fixture(scope="session")
def filesystem_name():
    return os.getenv("STORAGE_FILESYSTEM", "test")

@pytest.fixture(scope="session")
def datalake_client(account_name, account_key):
    account_url = "https://{}.dfs.core.windows.net/".format(account_name)
    client = DataLakeServiceClient(account_url=account_url, credential=account_key)
    yield client

@pytest.fixture(scope="session")
def filesystem(datalake_client, filesystem_name):
    filesystem_name = "test"
    fs_client = datalake_client.get_file_system_client(filesystem_name)

    dir_client = fs_client.get_directory_client("root/empty_dir")
    dir_client.create_directory()

    for filename in ["root/a/file.txt", "root/b/file.txt", "root/c/file.txt", "root/c/file.txt"]:
        file_client = fs_client.create_file(filename)
        data = b'some text'
        file_client.append_data(data, 0, len(data))
        file_client.flush_data(len(data))

    yield fs_client

@pytest.fixture(scope="function")
def filesystem_volatile(filesystem):
    dir_client = filesystem.get_directory_client("root/volatile")
    try:
        dir_client.delete_directory()
    except:
        pass
    dir_client.create_directory()
    yield
    try:
        dir_client.delete_directory()
    except:
        pass
