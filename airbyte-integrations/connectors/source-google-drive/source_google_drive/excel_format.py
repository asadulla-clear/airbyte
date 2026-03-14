
from airbyte_cdk.utils.oneof_option_config import OneOfOptionConfig
from pydantic.v1 import BaseModel, Field
from typing import Optional

class ExcelFormat(BaseModel):
    class Config(OneOfOptionConfig):
        title = "Excel Format"
        discriminator = "filetype"

    filetype: str = Field(
        "excel",
        const=True,
    )

    sheet_name: Optional[str] = Field(
        title="Sheet Name",
        description="The name of the sheet you want to sync. Leave empty to sync all sheets.",
        default=None,
    )
