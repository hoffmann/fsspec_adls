fsspec implementation for Azure Data Lake Store Gen2
====================================================

See:

- https://filesystem-spec.readthedocs.io/en/latest/index.html
- https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/storage/azure-storage-file-datalake
- https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-file-datalake/12.0.0b5/azure.storage.filedatalake.html
- https://github.com/dask/adlfs

To run the tests you need a [Azure Data Lake Gen2 Account](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) with hierarchical namespace enabled:

    export STORAGE_ACCOUNT_NAME=byblob
    export STORAGE_ACCOUNT_KEY=

    pip install -r requirements.txt
    pytest test_fsspec_adls.py -vv