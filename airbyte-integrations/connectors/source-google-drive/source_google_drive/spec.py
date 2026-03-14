#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

import dpath.util
from pydantic.v1 import BaseModel, Field

from airbyte_cdk import OneOfOptionConfig
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import (
    AbstractFileBasedSpec,
    DeliverRawFiles,
    DeliverRecords,
)
from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import (
    DeliverPermissions as DeliverPermissionsBase,
)
from airbyte_cdk.sources.file_based.config.avro_format import AvroFormat
from airbyte_cdk.sources.file_based.config.csv_format import CsvFormat
from airbyte_cdk.sources.file_based.config.jsonl_format import JsonlFormat
from airbyte_cdk.sources.file_based.config.parquet_format import ParquetFormat
from airbyte_cdk.sources.file_based.config.unstructured_format import UnstructuredFormat
from airbyte_cdk.sources.file_based.config.file_based_stream_config import (
    FileBasedStreamConfig as FileBasedStreamConfigBase,
)
from .excel_format import ExcelFormat


class RemoteIdentityType(Enum):
    USER = "user"
    GROUP = "group"


class RemoteIdentity(BaseModel):
    id: uuid.UUID
    remote_id: str
    parent_id: str | None = None
    name: str | None = None
    description: str | None = None
    email_address: str | None = None
    member_email_addresses: list[str] | None = None
    type: RemoteIdentityType
    modified_at: datetime


class RemotePermissions(BaseModel):
    id: str
    file_path: str
    allowed_identity_remote_ids: list[str] | None = None
    denied_identity_remote_ids: list[str] | None = None
    publicly_accessible: bool = False


class DeliverPermissions(DeliverPermissionsBase):
    domain: Optional[str] = Field(title="Domain", description="The Google domain of the identities.", airbyte_hidden=False, order=1)


class OAuthCredentials(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Authenticate via Google (OAuth)"
        discriminator = "auth_type"

    auth_type: Literal["Client"] = Field("Client", const=True)
    client_id: str = Field(
        title="Client ID",
        description="Client ID for the Google Drive API",
        airbyte_secret=True,
    )
    client_secret: str = Field(
        title="Client Secret",
        description="Client Secret for the Google Drive API",
        airbyte_secret=True,
    )
    refresh_token: str = Field(
        title="Refresh Token",
        description="Refresh Token for the Google Drive API",
        airbyte_secret=True,
    )


class ServiceAccountCredentials(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Service Account Key Authentication"
        discriminator = "auth_type"

    auth_type: Literal["Service"] = Field("Service", const=True)
    service_account_info: str = Field(
        title="Service Account Information",
        description='The JSON key of the service account to use for authorization. Read more <a href="https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating_service_account_keys">here</a>.',
        airbyte_secret=True,
    )


class FileBasedStreamConfig(FileBasedStreamConfigBase):
    format: Union[
        AvroFormat, CsvFormat, JsonlFormat, ParquetFormat, UnstructuredFormat, ExcelFormat
    ] = Field(
        title="Format",
        description="The configuration options that are used to alter how to read incoming files that deviate from the standard formatting.",
    )


class SourceGoogleDriveSpec(AbstractFileBasedSpec, BaseModel):
    class Config:
        title = "Google Drive Source Spec"

    streams: List[FileBasedStreamConfig] = Field(
        title="The list of streams to sync",
        description='Each instance of this configuration defines a <a href="https://docs.airbyte.com/cloud/core-concepts#stream">stream</a>. Use this to define which files belong in the stream, their format, and how they should be parsed and validated. When sending data to warehouse destination such as Snowflake or BigQuery, each stream is a separate table.',
        order=10,
    )

    folder_url: str = Field(
        description="URL for the folder you want to sync. Using individual streams and glob patterns, it's possible to only sync a subset of all files located in the folder.",
        examples=["https://drive.google.com/drive/folders/1Xaz0vXXXX2enKnNYU5qSt9NS70gvMyYn"],
        order=0,
        pattern="^https://drive.google.com/.+",
        pattern_descriptor="https://drive.google.com/drive/folders/MY-FOLDER-ID",
    )

    delivery_method: DeliverRecords | DeliverRawFiles | DeliverPermissions = Field(
        title="Delivery Method",
        discriminator="delivery_type",
        type="object",
        order=1,
        display_type="radio",
        group="advanced",
        default="use_records_transfer",
    )

    credentials: Union[OAuthCredentials, ServiceAccountCredentials] = Field(
        title="Authentication", description="Credentials for connecting to the Google Drive API", discriminator="auth_type", type="object"
    )

    @classmethod
    def documentation_url(cls) -> str:
        return "https://docs.airbyte.com/integrations/sources/google-drive"

    @classmethod
    def schema(cls, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Generates the mapping comprised of the config fields
        """
        schema = super().schema(*args, **kwargs)

        # Remove legacy settings
        dpath.util.delete(schema, "properties/streams/items/properties/legacy_prefix")
        dpath.util.delete(schema, "properties/streams/items/properties/format/oneOf/*/properties/inference_type")

        # Hide API processing option until https://github.com/airbytehq/airbyte-platform-internal/issues/10354 is fixed
        # Unstructured is usually at index 4, but let's be careful or just skip this if it fails
        try:
            processing_options = dpath.util.get(schema, "properties/streams/items/properties/format/oneOf/4/properties/processing/oneOf")
            dpath.util.set(schema, "properties/streams/items/properties/format/oneOf/4/properties/processing/oneOf", processing_options[:1])
        except Exception:
            pass

        return schema
