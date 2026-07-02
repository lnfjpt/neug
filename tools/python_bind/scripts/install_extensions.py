#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Install NeuG extensions (httpfs, parquet, gds) from OSS.

Used by nightly-extension-test.yml to download pre-built extension
binaries before running extension tests.  This is necessary because
the neug PyPI wheel does not ship compiled extension binaries.
"""

import logging
import sys

from neug import Database

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EXTENSIONS = ["httpfs", "parquet", "gds"]


def install_extensions():
    db = Database(":memory:", mode="w")
    conn = db.connect()
    for ext in EXTENSIONS:
        logger.info("Installing extension: %s", ext)
        conn.execute(f"INSTALL {ext}")
        logger.info("Extension %s installed successfully", ext)
    logger.info("All extensions installed successfully")


if __name__ == "__main__":
    try:
        install_extensions()
    except Exception as exc:
        logger.error("Failed to install extensions: %s", exc)
        sys.exit(1)
