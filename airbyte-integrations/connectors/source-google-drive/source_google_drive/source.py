#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import logging
import re
from typing import Any, List, Mapping, Optional

from airbyte_cdk import AdvancedAuth, ConfiguredAirbyteCatalog, ConnectorSpecification, OAuthConfigSpecification, TState
from airbyte_cdk.models import AuthFlowType, OauthConnectorInputSpecification
from airbyte_cdk.sources.file_based.file_based_source import FileBasedSource
from airbyte_cdk.sources.file_based.stream.cursor.default_file_based_cursor import DefaultFileBasedCursor
from airbyte_cdk.sources.file_based.file_types import default_parsers
from source_google_drive.spec import SourceGoogleDriveSpec
from source_google_drive.stream_permissions_reader import SourceGoogleDriveStreamPermissionsReader
from source_google_drive.stream_reader import SourceGoogleDriveStreamReader
from .excel_format import ExcelFormat
from .excel_parser import ExcelParser


class SourceGoogleDrive(FileBasedSource):
    def __init__(self, catalog: Optional[ConfiguredAirbyteCatalog], config: Optional[Mapping[str, Any]], state: Optional[TState]):
        parsers = {**default_parsers, ExcelFormat: ExcelParser()}
        super().__init__(
            stream_reader=SourceGoogleDriveStreamReader(),
            spec_class=SourceGoogleDriveSpec,
            catalog=catalog,
            config=config,
            state=state,
            cursor_cls=DefaultFileBasedCursor,
            stream_permissions_reader=SourceGoogleDriveStreamPermissionsReader(),
            parsers=parsers,
        )
        self._parsed_config = None
        self._sheets_cache = {}  # Cache discovered sheets by file ID

    def _get_parsed_config(self, config: Mapping[str, Any]) -> "SourceGoogleDriveSpec":
        """
        Parse the raw config dict into a SourceGoogleDriveSpec.
        We override this method to dynamically discover and expand 'split_sheets' Excel streams 
        into multiple concrete streams BEFORE the CDK base class's `streams` loop process them.
        """
        if self._parsed_config:
            return self._parsed_config

        # Call the base class parser to get the initial spec
        parsed_config = super()._get_parsed_config(config)
        
        # Ensure we have the stream reader configured for our dynamic discovery
        if not getattr(self.stream_reader, "config", None):
            self.stream_reader.config = parsed_config

        logger = getattr(self, "logger", logging.getLogger("airbyte"))
        expanded_streams = []

        for stream_config in getattr(parsed_config, "streams", []):
            format_config = getattr(stream_config, "format", None)
            is_excel = isinstance(format_config, ExcelFormat)
            split_sheets = is_excel and format_config.split_sheets

            if is_excel and split_sheets:
                stream_name = getattr(stream_config, "name", "unnamed")

                try:
                    globs = getattr(stream_config, "globs", []) or []
                    files = list(self.stream_reader.get_matching_files(globs, None, logger))

                    if not files:
                        expanded_streams.append(stream_config)
                        continue
                    
                    found_any_sheets = False
                    for file in files:
                        file_id = getattr(file, "id", None)
                        file_uri = getattr(file, "uri", "unknown_file")
                        # Extract the filename without extension for the stream name
                        filename = file_uri.split("/")[-1].split(".")[0]
                        sanitized_filename = re.sub(r"[^a-zA-Z0-9_]", "_", filename)
                        
                        if file_id and file_id in self._sheets_cache:
                            sheets = self._sheets_cache[file_id]
                        else:
                            logger.info(f"Dynamic sheet discovery enabled for stream '{stream_name}' in file '{file_uri}'")
                            excel_parser = self.parsers.get(type(format_config))
                            if not excel_parser:
                                continue

                            sheets = excel_parser.get_sheets(file, self.stream_reader, logger)
                            if file_id:
                                self._sheets_cache[file_id] = sheets

                        if not sheets:
                            continue

                        logger.info(f"Using {len(sheets)} sheets for file '{file_uri}': {sheets}")
                        
                        for sheet in sheets:
                            found_any_sheets = True
                            # Deep copy the stream_config object for each sheet
                            new_stream_config = stream_config.copy(deep=True)
                            sanitized_sheet = re.sub(r"[^a-zA-Z0-9_]", "_", str(sheet))
                            
                            # Stream name format: {base_name}_{file_name}_{sheet_name}
                            new_stream_config.name = f"{stream_config.name}_{sanitized_filename}_{sanitized_sheet}"
                            
                            # RESTRICT this stream to only read from this specific file!
                            new_stream_config.globs = [file_uri]
                            
                            # Explicitly configure the format to point to this single sheet
                            new_stream_config.format.sheet_name = sheet
                            new_stream_config.format.split_sheets = False
                            
                            expanded_streams.append(new_stream_config)

                    if not found_any_sheets:
                        # Fallback if discovery didn't find any sheets in any file
                        fallback_config = stream_config.copy(deep=True)
                        fallback_config.format.split_sheets = False
                        expanded_streams.append(fallback_config)
                except Exception as e:
                    logger.error(f"Error during dynamic discovery for '{stream_name}': {e}", exc_info=False)
                    fallback_config = stream_config.copy(deep=True)
                    fallback_config.format.split_sheets = False
                    expanded_streams.append(fallback_config)
            else:
                expanded_streams.append(stream_config)

        # Replace the original streams with our dynamically expanded streams
        parsed_config.streams = expanded_streams
        self._parsed_config = parsed_config
        return parsed_config

    def spec(self, *args: Any, **kwargs: Any) -> ConnectorSpecification:
        """
        Returns the specification describing what fields can be configured by a user when setting up a file-based source.
        """
        oauth_connector_input_specification = OauthConnectorInputSpecification(
            consent_url="https://accounts.google.com/o/oauth2/v2/auth?{{client_id_param}}&{{redirect_uri_param}}&response_type=code&{{scope_param}}&access_type=offline&{{state_param}}&include_granted_scopes=true&prompt=consent",
            access_token_url="https://oauth2.googleapis.com/token?{{client_id_param}}&{{client_secret_param}}&{{auth_code_param}}&{{redirect_uri_param}}&grant_type=authorization_code",
            scope="https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/admin.directory.group.readonly https://www.googleapis.com/auth/admin.directory.group.member.readonly https://www.googleapis.com/auth/admin.directory.user.readonly",
        )

        return ConnectorSpecification(
            documentationUrl=self.spec_class.documentation_url(),
            connectionSpecification=self.spec_class.schema(),
            advanced_auth=AdvancedAuth(
                auth_flow_type=AuthFlowType.oauth2_0,
                predicate_key=["credentials", "auth_type"],
                predicate_value="Client",
                oauth_config_specification=OAuthConfigSpecification(
                    oauth_connector_input_specification=oauth_connector_input_specification,
                    complete_oauth_output_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "refresh_token": {
                                "type": "string",
                                "path_in_connector_config": ["credentials", "refresh_token"],
                                "path_in_oauth_response": ["refresh_token"],
                            }
                        },
                    },
                    complete_oauth_server_input_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"client_id": {"type": "string"}, "client_secret": {"type": "string"}},
                    },
                    complete_oauth_server_output_specification={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "client_id": {"type": "string", "path_in_connector_config": ["credentials", "client_id"]},
                            "client_secret": {"type": "string", "path_in_connector_config": ["credentials", "client_secret"]},
                        },
                    },
                ),
            ),
        )
