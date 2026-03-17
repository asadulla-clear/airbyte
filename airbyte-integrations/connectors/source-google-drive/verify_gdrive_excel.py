
import pandas as pd
import io
import logging
from typing import Optional, List
from unittest.mock import MagicMock
import sys
import os

# Stub out CDK modules to avoid dependency issues
from types import ModuleType
class MockPackage(ModuleType):
    def __init__(self, name):
        super().__init__(name)
        self.__path__ = []

def mock_pkg(name):
    m = MockPackage(name)
    sys.modules[name] = m
    return m

# Inject MockBase into the parent classes
class MockBase: pass

mock_pkg("airbyte_cdk")
mock_pkg("airbyte_cdk.utils")
mock_pkg("airbyte_cdk.utils.oneof_option_config")
mock_pkg("airbyte_cdk.sources")
mock_pkg("airbyte_cdk.sources.file_based")
mock_pkg("airbyte_cdk.sources.file_based.config")
mock_pkg("airbyte_cdk.sources.file_based.config.file_based_stream_config")
mock_pkg("airbyte_cdk.sources.file_based.file_based_stream_reader")
mock_pkg("airbyte_cdk.sources.file_based.file_types")
import airbyte_cdk.sources.file_based.file_based_stream_reader as fbsr
fbsr.AbstractFileBasedStreamReader = MockBase
class MockEnum: pass
fbsr.FileReadMode = MockEnum
fbsr.FileReadMode.READ_BINARY = "rb"

import airbyte_cdk.sources.file_based.remote_file as rf
rf.RemoteFile = MockBase

import airbyte_cdk.sources.file_based.schema_helpers as sh
sh.SchemaType = dict

# Inject MockBase into the parent classes
class MockBase: pass
import airbyte_cdk.sources.file_based.file_types.file_type_parser as ftp
ftp.FileTypeParser = MockBase

import airbyte_cdk.utils.oneof_option_config as ooc
ooc.OneOfOptionConfig = MockBase

import airbyte_cdk.sources.file_based.config.file_based_stream_config as fbsc
fbsc.FileBasedStreamConfig = MockBase

sys.modules["pydantic.v1"] = MagicMock()
import pydantic.v1 as p1
p1.BaseModel = MockBase
p1.Field = MagicMock()
p1.validator = MagicMock()

# Import our custom classes
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/source_google_drive")
from excel_format import ExcelFormat
from excel_parser import ExcelParser

def create_sample_xlsx(path: str):
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df2 = pd.DataFrame({"colA": [10, 20], "colB": ["X", "Y"]})
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df1.to_excel(writer, sheet_name="Sheet1", index=False)
        df2.to_excel(writer, sheet_name="Sheet2", index=False)
    print(f"Created sample XLSX at {path}")

def verify():
    path = "/tmp/test_multi_sheet.xlsx"
    create_sample_xlsx(path)
    
    # Mock RemoteFile
    file = MagicMock()
    file.uri = path
    
    # Mock StreamReader
    stream_reader = MagicMock()
    stream_reader.open_file.return_value.__enter__.return_value = open(path, "rb")
    
    # Mock Config
    config = MagicMock()
    config.format = ExcelFormat(sheet_name=None) # Sync all sheets
    
    logger = logging.getLogger("test")
    parser = ExcelParser()
    
    print("Testing sync all sheets (sheet_name=None)...")
    records = list(parser._read_excel(config, file, stream_reader, logger))
    print(f"Total records found: {len(records)}")
    for r in records:
        print(r)
    
    assert len(records) == 4, "Should have 4 records total (2 from each sheet)"
    
    print("\nTesting sync specific sheet (sheet_name='Sheet2')...")
    stream_reader.open_file.return_value.__enter__.return_value = open(path, "rb") # Reset iterator
    config.format = ExcelFormat(sheet_name="Sheet2")
    records2 = list(parser._read_excel(config, file, stream_reader, logger))
    print(f"Total records found: {len(records2)}")
    for r in records2:
        print(r)
    
    assert len(records2) == 2, "Should have only 2 records from Sheet2"
    assert "colA" in records2[0], "Records should be from Sheet2"
    
    print("\nVerification SUCCESS!")

if __name__ == "__main__":
    verify()
