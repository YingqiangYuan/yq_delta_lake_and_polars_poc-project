# -*- coding: utf-8 -*-

import typing as T
from functools import cached_property

import boto3
from s3pathlib import context
from boto_session_manager import BotoSesManager

if T.TYPE_CHECKING:  # pragma: no cover
    from .one_01_main import One


class OneBotoSesMixin:
    @cached_property
    def bsm(self: "One") -> BotoSesManager:
        bsm = BotoSesManager(
            profile_name="yuan_yingqiang_dev",
            region_name="us-east-1",
        )
        context.attach_boto_session(bsm.boto_ses)
        return bsm

    @cached_property
    def boto_ses(self: "One") -> boto3.Session:
        return self.bsm.boto_ses

    @cached_property
    def s3_client(self: "One"):
        return self.bsm.s3_client

    @cached_property
    def polars_storage_options(self: "One") -> dict:
        creds = self.bsm.boto_ses.get_credentials().get_frozen_credentials()
        storage_options = {
            "AWS_REGION": self.bsm.aws_region,
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            "AWS_SESSION_TOKEN": creds.token or "",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # skip DynamoDB lock
        }
        return storage_options
