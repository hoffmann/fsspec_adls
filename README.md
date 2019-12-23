fsspec implementation for Azure Data Lake Store Gen2
====================================================

See:

- https://filesystem-spec.readthedocs.io/en/latest/index.html
- https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage/azure-storage-file-datalake
- https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-file-datalake/12.0.0b5/azure.storage.filedatalake.html
- https://github.com/dask/adlfs

To run the tests you need a [Azure Data Lake Gen2 Account](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) with hierarchical namespace enabled:

```bash
export STORAGE_ACCOUNT_NAME=xxx
export STORAGE_ACCOUNT_KEY=yyy

pip install -r requirements.txt
pytest test_fsspec_adls.py -vv
```

Example:

```python
import os
from fsspec_adls import DataLakeFileSystem

account_name = os.getenv("STORAGE_ACCOUNT_NAME")
account_key = os.getenv("STORAGE_ACCOUNT_KEY")
account_url = "https://{}.dfs.core.windows.net/".format(account_name)

dfs = DataLakeFileSystem(account_url, credential, "filesystem")

dfs.mkdir("testdir")

with dfs.open("testdir/hello.txt", "wb") as f:
    f.write(b"world")

dfs.ls("testdir")
dfs.mv("testidr", "newdir")

with dfs.open("newdir/hello.txt", "rb") as f:
    print(f.read())
```
