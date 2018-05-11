# Copyright (c) 2017-2018 Neogeo-Technologies.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from source import Source
from protocol import json, wfs
from unittest.mock import patch

import unittest


class TestSource(unittest.TestCase):

    def test_new_json(self):
        s = Source("http://foo.bar", "json")
        self.assertTrue(type(s), json.Source)

    def test_wfs_creation(self):
        # mock is mandatory as the instantiation tries to connect to a WebService
        with patch("onegeo_manager.protocol.wfs.Source.__init__", lambda x, y: None):
            s = Source("http://foo.bar", "wfs")
            self.assertTrue(type(s), wfs.Source)

    def test_new_with_import_error(self):
        self.assertRaises(ImportError, Source, "http", "foo")


if __name__ == "__main__":
    unittest.main()
