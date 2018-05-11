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


import json
import os
import unittest
from pathlib import Path

from index_profile import IndexProfile
from source import Source


class TestJsonRessource(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tmp_source_file = "tmp.json"
        cls.foo_res = {
            "test": {
                "foo": [1234, 5678],
                "bar": [1.0, 2.0]
            }
        }

        with open(cls.tmp_source_file, "w") as f:
            json.dump(cls.foo_res, f)
        cls.json_resources = Source(Path(os.path.abspath(cls.tmp_source_file)).as_uri(), "json").get_resources()
        cls.idx_profile = IndexProfile("foo_profile", cls.json_resources[0])

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.tmp_source_file)

    def test_resource_correct(self):
        names = [val["name"] for val in TestJsonRessource.json_resources[0].columns]
        self.assertEqual("test", TestJsonRessource.json_resources[0].path)
        self.assertEqual(2, len(TestJsonRessource.json_resources[0].columns))
        self.assertTrue("foo" in names)
        self.assertTrue("bar" in names)

    def test_gen_profile(self):
        # The profile output seems odd, should be a number type for the columns foo and bar no?
        expected_elastic_profile = {"foo_profile": {'properties': {'lineage': {'properties': {'source': {'properties': {
            'protocol': {'type': 'keyword', 'index': 'not_analyzed', 'store': False, 'include_in_all': False},
            'uri': {'type': 'keyword', 'index': 'not_analyzed', 'store': False, 'include_in_all': False}}}}},
            'properties': {'properties': {'foo': {'type': 'text', 'fields': {
                'keyword': {'type': 'keyword', 'ignore_above': 256}}},
                                          'bar': {'type': 'text', 'fields': {
                                              'keyword': {'type': 'keyword',
                                                          'ignore_above': 256}}}}}}}}
        final_profile = TestJsonRessource.idx_profile.generate_elastic_mapping()
        self.assertEqual(expected_elastic_profile, final_profile)

    def test_data_collect(self):
        # This test does not work, need more info
        collection = TestJsonRessource.idx_profile.get_collection()
        #
        # for c in collection:
        #     print(c)
        # self.assertEqual(
        #     Source(Path(os.path.abspath(TestJsonRessource.tmp_source_file)).as_uri(), "json")._data["test"], collection)
        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main()
