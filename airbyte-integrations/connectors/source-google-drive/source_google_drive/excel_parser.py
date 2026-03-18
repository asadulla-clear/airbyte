
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
    def __init__(self):
        super().__init__()
        self._file_cache: Dict[str, bytes] = {}

    def _get_file_content(
        self,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> bytes:
        """
        Fetch file content from cache or download it if not present.
        """
        file_id = getattr(file, "id", file.uri)
        if file_id in self._file_cache:
            return self._file_cache[file_id]

        with stream_reader.open_file(file, self.file_read_mode, None, logger) as fp:
            content = fp.read()
            self._file_cache[file_id] = content
            return content

    def check_config(self, config: FileBasedStreamConfig) -> Tuple[bool, Optional[str]]:
        return True, None

    def get_sheets(
        self,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> Iterable[str]:
        from pyxlsb import open_workbook
        from python_calamine import CalamineWorkbook

        file_extension = file.uri.split(".")[-1].lower() if "." in file.uri else ""
        content = self._get_file_content(file, stream_reader, logger)

        if file_extension == "xlsb":
            try:
                with open_workbook(io.BytesIO(content)) as wb:
                    return [str(s) for s in wb.sheets]
            except Exception as e:
                logger.error(f"Error reading XLSB sheets for {file.uri}: {e}")
                return []
        else:
            try:
                # Use Calamine directly to get sheet names - much faster and more reliable than Polars for metadata
                wb = CalamineWorkbook.from_filelike(io.BytesIO(content))
                return wb.sheet_names
            except Exception as e:
                logger.error(f"Error discovering XLSX/XLS sheets for {file.uri}: {e}")
                # Fallback to Polars if Calamine wrapper fails for some reason
                try:
                    import polars as pl
                    df_dict = pl.read_excel(io.BytesIO(content), sheet_name=None)
                    if isinstance(df_dict, dict):
                        return list(df_dict.keys())
                except:
                    pass
                return []

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
        from pyxlsb import open_workbook

        excel_format = config.format
        sheet_name = excel_format.sheet_name if isinstance(excel_format, ExcelFormat) else None
        file_extension = file.uri.split(".")[-1].lower() if "." in file.uri else ""
        
        content = self._get_file_content(file, stream_reader, logger)

        if file_extension == "xlsb":
            # Native XLSB streaming to avoid OOM and phantom columns
            try:
                with open_workbook(io.BytesIO(content)) as wb:
                    sheets_to_read = [sheet_name] if sheet_name else wb.sheets
                    
                    count = 0
                    for sn in sheets_to_read:
                        try:
                            with wb.get_sheet(sn) as sheet:
                                headers = []
                                for row in sheet.rows():
                                    # Convert row cells to values
                                    row_values = [c.v for c in row]
                                    
                                    # Skip completely empty rows
                                    if not any(v is not None for v in row_values):
                                        continue
                                        
                                    # Trim trailing Nones to avoid "phantom" columns
                                    while row_values and row_values[-1] is None:
                                        row_values.pop()
                                    
                                    if not headers:
                                        # Header row: ensure unique and non-empty keys
                                        headers = [str(v) if v is not None else f"Unnamed: {i}" for i, v in enumerate(row_values)]
                                        continue
                                    
                                    # Data row
                                    record = {}
                                    has_data = False
                                    for i, h in enumerate(headers):
                                        val = row_values[i] if i < len(row_values) else None
                                        if val is not None:
                                            has_data = True
                                        record[h] = val
                                    
                                    if has_data:
                                        record["excel_sheet_name"] = str(sn)
                                        yield record
                                        count += 1
                                        if limit and count >= limit:
                                            return
                        except Exception as e:
                            logger.warning(f"Could not read sheet {sn} in {file.uri}: {e}")
                            continue
            except Exception as e:
                logger.error(f"Error streaming XLSB file {file.uri}: {e}")
                return
        else:
            # High-performance Polars + Calamine engine for XLSX/XLSX
            import polars as pl
            try:
                data_stream = io.BytesIO(content)
                
                # Read all sheets or specific sheet using Polars
                # The 'calamine' engine is written in Rust and is extremely fast.
                try:
                    df_dict = pl.read_excel(
                        data_stream, 
                        sheet_name=sheet_name if sheet_name else None,
                        engine="calamine"
                    )
                except Exception as e:
                    logger.warning(f"Calamine engine failed, falling back to default: {e}")
                    df_dict = pl.read_excel(data_stream, sheet_name=sheet_name if sheet_name else None)
                    
                data_stream.close()
            except Exception as e:
                logger.error(f"Error reading Excel file {file.uri} via Polars: {e}")
                return

            def process_df(df: pl.DataFrame, label: str) -> Iterable[Dict[str, Any]]:
                # Polars equivalent of dropna(how="all")
                # We filter out rows where all columns are null
                df = df.filter(~pl.all_horizontal(pl.all().is_null()))
                
                # Convert to list of dicts for Airbyte
                for r in df.to_dicts():
                    # Add sheet name and filter out "phantom" null columns
                    cleaned_record = {k: v for k, v in r.items() if v is not None}
                    if cleaned_record:
                        cleaned_record["excel_sheet_name"] = label
                        yield cleaned_record

            if isinstance(df_dict, pl.DataFrame):
                sheet_label = str(sheet_name) if sheet_name else "0"
                yield from process_df(df_dict, sheet_label)
            elif isinstance(df_dict, dict):
                count = 0
                for s, df in df_dict.items():
                    for record in process_df(df, str(s)):
                        yield record
                        count += 1
                        if limit and count >= limit:
                            return
