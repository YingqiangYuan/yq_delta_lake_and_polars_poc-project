# -*- coding: utf-8 -*-

from .one_02_config import OneConfigMixin
from .one_03_boto_ses import OneBotoSesMixin
from .one_04_s3 import OneS3Mixin


class One(
    OneConfigMixin,
    OneBotoSesMixin,
    OneS3Mixin,
):
    pass

one = One()
