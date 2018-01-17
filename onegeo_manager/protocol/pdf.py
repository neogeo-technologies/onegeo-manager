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


from base64 import b64encode
from functools import wraps
from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.index_profile import fetch_mapping
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import clean_my_obj
import operator
from pathlib import Path
from PyPDF2 import PdfFileReader


class Resource(AbstractResource):

    def __init__(self, source, name):
        super().__init__(source, name)
        self.add_column('data', column_type='pdf', occurs=(1, 1))

    def authorized_column_type(self, val):
        return val in operator.add(self.COLUMN_TYPE, ['pdf'])


class Source(AbstractSource):

    META_FIELD = ('Author', 'CreationDate', 'Creator', 'Keywords',
                  'ModDate', 'Producer', 'Subject', 'Title')

    def __init__(self, path, name):

        self.__p = Path(path.startswith('file://') and path[7:] or path)
        if not self.__p.exists():
            raise ConnectionError('The given path does not exist.')

        super().__init__(self.__p.as_uri(), name)

    def _iter_pdf_path(self, subdir_name=None):
        if subdir_name:
            path = '{0}/**/*.pdf'.format(subdir_name)
        else:
            path = '**/*.pdf'
        return iter(list(self.__p.glob(path)))

    def _iter_dir_path(self):
        subdirs = [sub for sub in self.__p.iterdir() if sub.is_dir()]
        if subdirs:
            return iter(subdirs)
        return iter([self.__p])

    def get_resources(self):

        arr = []
        for subdir in self._iter_dir_path():
            if subdir == self.__p:
                iter_pdf_path = self._iter_pdf_path()
            else:
                iter_pdf_path = self._iter_pdf_path(subdir_name=subdir.name)
            columns = {}
            resource = Resource(self, subdir.name)
            for p in iter_pdf_path:
                pdf = PdfFileReader(open(p.as_posix(), 'rb'))
                for k, _ in pdf.getDocumentInfo().items():
                    k = k[1:]
                    if k in self.META_FIELD:
                        continue
                    if k in columns:
                        columns[k] += 1
                    else:
                        columns[k] = 1
            for column, count in columns.items():
                resource.add_column(column, count=count)
            arr.append(resource)

        return arr

    def get_collection(self, resource_name):

        def format(meta):
            copy = {}
            for k, v in meta.items():
                k = k[1:]
                if k in self.META_FIELD:
                    continue
                copy[k] = v
            return copy

        iter_pdf_path = None
        for subdir in self._iter_dir_path():
            if subdir == self.__p:
                iter_pdf_path = self._iter_pdf_path()
            elif subdir.name == resource_name:
                iter_pdf_path = self._iter_pdf_path(subdir_name=subdir.name)

        if not iter_pdf_path:
            raise ValueError('{0} not found.'.format(resource_name))

        for path in iter_pdf_path:
            f = open(path.as_posix(), 'rb')
            yield {'file': b64encode(f.read()).decode('utf-8'),
                   'filename': '/'.join(path.parts[len(self.__p.parts):]),
                   'properties': format(
                       dict(PdfFileReader(f).getDocumentInfo()))}


class IndexProfile(AbstractIndexProfile):

    META_FIELD = ('Author', 'CreationDate', 'Creator', 'Keywords',
                  'ModDate', 'Producer', 'Subject', 'Title')

    def __init__(self, name, elastic_index, resource):
        super().__init__(name, elastic_index, resource)

    def _format(f):

        @wraps(f)
        def wrapper(self, *args, **kwargs):

            def set_aliases(properties):
                new = {}
                for k, v in properties.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        continue
                    new[prop.alias or prop.name] = v
                return new

            for doc in f(self, *args, **kwargs):
                yield {
                    'origin': {
                        'filename': doc['filename'],
                        'resource': {
                            'name': self.resource.name},
                        'source': {
                            'name': self.resource.source.name,
                            'uri': self.resource.source.uri,
                            'protocol': self.resource.source.protocol}},
                    'properties': set_aliases(doc['properties']),
                    'raw_data': doc['file']}

        return wrapper

    @_format
    def get_collection(self):
        return self.resource.source.get_collection(self.resource.name)

    def generate_elastic_mapping(self):

        analyzer = self.elastic_index.analyzer
        search_analyzer = self.elastic_index.search_analyzer

        mapping = {
            self.name: {
                'properties': {
                    'filename': {
                        'include_in_all': False,
                        'index': 'not_analyzed',
                        'store': False,
                        'type': 'keyword'}}}}

        if self.tags:
            mapping[self.name]['properties']['tags'] = {
                'analyzer': analyzer,
                'boost': 1.0,
                # 'doc_value'
                # 'eager_global_ordinals'
                # 'fields'
                # 'ignore_above'
                # 'include_in_all'
                'index': True,
                'index_options': 'docs',
                'norms': True,
                # 'null_value'
                'store': False,
                'search_analyzer': search_analyzer,
                'similarity': 'classic',
                'term_vector': 'yes',
                'type': 'keyword'}

        props = {}
        for p in self.iter_properties():

            if p.column_type == 'pdf':
                mapping[self.name]['properties']['attachment'] = {
                    'properties': {
                        'content': {
                            'analyzer': p.analyzer,
                            'boost': p.weight,
                            # 'eager_global_ordinals'
                            # 'fielddata'
                            # 'fielddata_frequency_filter'
                            'fields': {
                                'keyword': {
                                    'index': 'not_analyzed',
                                    'type': 'keyword',
                                    'store': False}},
                            'include_in_all': False,
                            'index': True,
                            'index_options': 'offsets',
                            'norms': True,
                            'position_increment_gap': 100,
                            'store': False,
                            'search_analyzer': p.search_analyzer,
                            # 'search_quote_analyzer'
                            'similarity': 'classic',
                            'term_vector': 'yes',
                            'type': 'text'}}}
                continue

            if p.rejected:
                continue

            props[p.alias or p.name] = fetch_mapping(p)

        if props:
            mapping[self.name]['properties']['properties'] = {
                'properties': props}

        mapping[self.name]['properties']['origin'] = {
            'properties': {
                'resource': {
                    'properties': {
                        'name': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'}}},
                'source': {
                    'properties': {
                        'name': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'},
                        'type': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'},
                        'uri': {
                            'include_in_all': False,
                            'index': 'not_analyzed',
                            'store': False,
                            'type': 'keyword'}}}}}

        return clean_my_obj(mapping)
