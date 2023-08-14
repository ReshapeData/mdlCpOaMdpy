#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlCpOaMdpy import *
import pytest

@pytest.mark.parametrize('token,FDate,output',
                         [("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "2023-06-01", "该编码GYS001221已存在于金蝶"),
                          ("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "2023-06-01", "没有需要同步的供应商")])
def test_supplierByDate_sync(token, FDate, output):
    assert supplierByDate_sync(token, FDate) == output

