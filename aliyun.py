# -*- coding: utf-8 -*-
# This file is auto-generated, don't edit it. Thanks.
import os
import sys
import jsonlines
from typing import List

from alibabacloud_ocr_api20210707.client import Client as ocr_api20210707Client
from alibabacloud_credentials.client import Client as CredentialClient
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_darabonba_stream.client import Client as StreamClient
from alibabacloud_ocr_api20210707 import models as ocr_api_20210707_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient


class OCR:
    def __init__(self, outdir):
        os.makedirs(outdir, exist_ok=True)
        self.outdir = outdir
        self.client = self.create_client()

    def create_client(self) -> ocr_api20210707Client:
        """
        使用凭据初始化账号Client
        @return: Client
        @throws Exception
        """
        # 工程代码建议使用更安全的无AK方式，凭据配置方式请参见：https://help.aliyun.com/document_detail/378659.html。
        # credential = CredentialClient()
        # config = open_api_models.Config(
        #     credential=credential
        # )
        config = open_api_models.Config(
            # 您的AccessKey ID,
            access_key_id="LTAI5t6ajBimLh4QJZZ2p9Av",
            # 您的AccessKey Secret,
            access_key_secret="j6AkLfueoOZkuubIpYObCVujbzhHi3"
        )
        # Endpoint 请参考 https://api.aliyun.com/product/ocr-api
        config.endpoint = f'ocr-api.cn-hangzhou.aliyuncs.com'
        return ocr_api20210707Client(config)

    def parse(self, idx, path) -> None:
        result_path = f"{self.outdir}/result_{idx}.jsonl"
        err_path = f"{self.outdir}/error_{idx}.log"
        if os.path.exists(result_path):
            with jsonlines.open(result_path, "r") as rf:
                for item in rf:
                    image_path = item["image_path"]
                    if image_path == path:
                        # print(f"{path} exists.")
                        return item

        # 需要安装额外的依赖库，直接点击下载完整工程即可看到所有依赖。
        body_stream = StreamClient.read_from_file_path(path)
        recognize_all_text_request = ocr_api_20210707_models.RecognizeAllTextRequest(
            body=body_stream,
            type='General'
        )
        runtime = util_models.RuntimeOptions()
        try:
            res = self.client.recognize_all_text_with_options(recognize_all_text_request, runtime)
            res = res.to_map()
            res["image_path"] = path
            with jsonlines.open(result_path, "w") as wf:
                wf.write(res)
        except Exception as error:
            # 错误 message
            with open(err_path, "w") as wf:
                wf.write(f"msg={error.message}, address={error.data.get('Recommend')}")
            UtilClient.assert_as_string(error.message)
            res = None
        return res

