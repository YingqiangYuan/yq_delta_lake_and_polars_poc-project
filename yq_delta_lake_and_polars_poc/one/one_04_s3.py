# -*- coding: utf-8 -*-

import typing as T
from functools import cached_property

from s3pathlib import S3Path

if T.TYPE_CHECKING:  # pragma: no cover
    from .one_01_main import One


class OneS3Mixin:
    @cached_property
    def s3_bucket(self: "One") -> str:
        return f"{self.bsm.aws_account_alias}-{self.bsm.aws_region}-data"

    @cached_property
    def s3dir_root(self: "One") -> S3Path:
        return S3Path(
            f"s3://{self.s3_bucket}/projects/yq_delta_lake_and_polars_poc/"
        ).to_dir()
