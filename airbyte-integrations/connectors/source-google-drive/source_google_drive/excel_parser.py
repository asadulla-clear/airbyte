
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
        from pyxlsb import open_workbook

        excel_format = config.format
        sheet_name = excel_format.sheet_name if isinstance(excel_format, ExcelFormat) else None
        
        file_extension = file.uri.split(".")[-1].lower() if "." in file.uri else ""

        if file_extension == "xlsb":
            # Native XLSB streaming to avoid OOM and phantom columns
            with stream_reader.open_file(file, self.file_read_mode, None, logger) as fp:
                try:
                    # Still need to read fully if seeking is not supported, 
                    # but we avoid pandas full grid allocation.
                    content = fp.read()
                    with open_workbook(io.BytesIO(content)) as wb:
                        del content
                        
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
            # Fallback for XLSX/XLS using pandas
            with stream_reader.open_file(file, self.file_read_mode, None, logger) as fp:
                try:
                    content = fp.read()
                    data_stream = io.BytesIO(content)
                    del content
                    df_dict = pd.read_excel(data_stream, sheet_name=sheet_name if sheet_name else None)
                    data_stream.close()
                except Exception as e:
                    logger.error(f"Error reading Excel file {file.uri}: {e}")
                    return

                def clean_df(df: pd.DataFrame) -> pd.DataFrame:
                    df = df.dropna(how="all", axis=0).dropna(how="all", axis=1)
                    unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed:")]
                    for col in unnamed_cols:
                        if df[col].isnull().all():
                            df = df.drop(columns=[col])
                    return df

                if isinstance(df_dict, pd.DataFrame):
                    df = clean_df(df_dict)
                    sheet_label = str(sheet_name) if sheet_name else "0"
                    for i, r in enumerate(df.to_dict(orient="records")):
                        r["excel_sheet_name"] = sheet_label
                        yield r
                        if limit and i + 1 >= limit:
                            return
                elif isinstance(df_dict, dict):
                    count = 0
                    for s, d_orig in df_dict.items():
                        df = clean_df(d_orig)
                        for r in df.to_dict(orient="records"):
                            r["excel_sheet_name"] = str(s)
                            yield r
                            count += 1
                            if limit and count >= limit:
                                return
