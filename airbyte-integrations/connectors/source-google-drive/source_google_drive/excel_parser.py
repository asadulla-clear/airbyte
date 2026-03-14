
import logging
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple
import pandas as pd
import io

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import SchemaType
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser

from .excel_format import ExcelFormat

class ExcelParser(FileTypeParser):
    def check_config(self, config: FileBasedStreamConfig) -> Tuple[bool, Optional[str]]:
        return True, None

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> SchemaType:
        # Aggregate all unique keys from the sampled records to handle multi-sheet schemas
        records = list(self._read_excel(config, file, stream_reader, logger, limit=1000))
        if not records:
            return {}
        
        all_keys = set()
        for r in records:
            all_keys.update(r.keys())
            
        schema = {}
        for key in sorted(list(all_keys)):
            schema[key] = {"type": "string"}
        return schema

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Optional[Mapping[str, SchemaType]],
    ) -> Iterable[Dict[str, Any]]:
        yield from self._read_excel(config, file, stream_reader, logger)

    @property
    def file_read_mode(self) -> FileReadMode:
        return FileReadMode.READ_BINARY

    def _read_excel(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        limit: Optional[int] = None
    ) -> Iterable[Dict[str, Any]]:
        excel_format = config.format
        if not isinstance(excel_format, ExcelFormat):
            # Fallback for if it's still the basic dict from CDK
            sheet_name = None
        else:
            sheet_name = excel_format.sheet_name

        with stream_reader.open_file(file, self.file_read_mode, None, logger) as fp:
            content = fp.read()
            # sheet_name in pd.read_excel can be None (all sheets as dict), 
            # a string (one sheet), or a list of strings/ints.
            # If sheet_name is None in our config, we want ALL sheets.
            # Pandas default for sheet_name is 0 (first sheet).
            # To get all sheets, we must pass sheet_name=None to pandas.
            
            target_sheet = sheet_name if sheet_name is not None else None
            
            try:
                df_dict = pd.read_excel(io.BytesIO(content), sheet_name=target_sheet)
            except Exception as e:
                logger.error(f"Error reading Excel file {file.uri}: {e}")
                return

            if isinstance(df_dict, pd.DataFrame):
                # Only one sheet was read. If sheet_name was provided, use it, else default to "0" (pandas default)
                sheet_label = str(sheet_name) if sheet_name is not None else "0"
                records = []
                for r in df_dict.to_dict(orient="records"):
                    r["excel_sheet_name"] = sheet_label
                    records.append(r)
            elif isinstance(df_dict, dict):
                # Multiple sheets were read
                records = []
                for sheet, df in df_dict.items():
                    if not df.empty:
                        sheet_records = df.to_dict(orient="records")
                        for r in sheet_records:
                            r["excel_sheet_name"] = str(sheet)
                            records.append(r)
            else:
                return

            for i, r in enumerate(records):
                yield r
                if limit and i + 1 >= limit:
                    return
