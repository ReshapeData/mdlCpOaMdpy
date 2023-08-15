#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mdlCpOaMdpy import *
import pytest

@pytest.mark.parametrize('token,FDate,output',
                         [("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "2023-06-01", "江苏领坤生物科技有限公司已存在于金蝶"),
                          ("9B6F803F-9D37-41A2-BDA0-70A7179AF0F3", "2023-06-01", "无需要同步的客户")])
def test_customerByDate_sync(token, FDate, output):

    assert customerByDate_sync(token, FDate) == output
