from typing import Optional, Type
import uuid
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase

from snakemake_storage_plugin_sftp import StorageProvider, StorageProviderSettings


class TestStorageBase(TestStorageBase):
    def get_query_not_existing(self, tmp_path) -> str:
        return f"sftp://demo.wftpserver.com:2222/upload/{uuid.uuid4()}.txt"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        return StorageProviderSettings(
            username="demo",
            password="demo",
            not_sync_mtime=True,
        )


class TestStorageStore(TestStorageBase):
    __test__ = True
    store_only = True
    delete = False

    def get_query(self, tmp_path) -> str:
        return "sftp://demo.wftpserver.com:2222/upload/snakemake-test.md"


class TestStorageRetrieve(TestStorageBase):
    __test__ = True
    retrieve_only = True

    def get_query(self, tmp_path) -> str:
        return "sftp://demo.wftpserver.com:2222/download/version.txt"
