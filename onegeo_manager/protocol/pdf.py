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
from onegeo_manager.index_profile import not_searchable
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import clean_my_obj
from onegeo_manager.utils import digest_binary
from pathlib import Path
from PyPDF2 import PdfFileReader
from PyPDF2.utils import PdfReadError


__description__ = 'PDF Store'


META_FIELD = (
    'Author', 'CreationDate', 'Creator', 'Keywords',
    'ModDate', 'Producer', 'Subject', 'Title')


class Resource(AbstractResource):

    def __init__(self, source, uri=None):
        self._uri = uri
        self._p = Path(uri[7:])
        super().__init__(source, name=self._p.name)

        self.title = self._p.name

        self.add_column(
            'attachment/author', column_type='text', occurs=(0, 1))
        self.add_column(
            'attachment/content', column_type='text', occurs=(1, 1))
        self.add_column(
            'attachment/content_length', column_type='long', occurs=(0, 1))
        self.add_column(
            'attachment/content_type', column_type='text', occurs=(0, 1))
        self.add_column(
            'attachment/date', column_type='date', occurs=(0, 1))
        self.add_column(
            'attachment/keywords', column_type='text', occurs=(0, 1))
        self.add_column(
            'attachment/language', column_type='text', occurs=(0, 1))
        self.add_column(
            'attachment/title', column_type='text', occurs=(0, 1))

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self):
        raise AttributeError("Attibute is locked, you can't modify it.")

    @uri.deleter
    def uri(self):
        raise AttributeError("Attibute is locked, you can't delete it.")

    def get_collection(self):

        for path in list(self._p.glob('**/*.[pP][dD][fF]')):
            filename = '/'.join(path.parts[len(self._p.parts):])
            with open(path.as_posix(), 'rb') as f:
                try:
                    info = PdfFileReader(f).getDocumentInfo()
                except PdfReadError:
                    info = {}

            with open(path.as_posix(), 'rb') as f:
                raw = b64encode(f.read()).decode('utf-8')

            properties = {}
            for k, v in info.items():
                k = k[1:]
                if k in META_FIELD:
                    continue
                properties[k] = v

            yield {'raw': raw,
                   'filename': filename,
                   'properties': properties}


class Source(AbstractSource):

    META_FIELD = ('Author', 'CreationDate', 'Creator', 'Keywords',
                  'ModDate', 'Producer', 'Subject', 'Title')

    def __init__(self, uri):
        self._p = Path(uri.startswith('file://') and uri[7:] or uri)
        if not self._p.exists():
            raise ConnectionError('The given path does not exist.')
        super().__init__(uri)

    def subdirectories(self):
        subdirs = [sub for sub in self._p.iterdir() if sub.is_dir()]
        if subdirs:
            return subdirs
        return [self._p]

    def get_resources(self, *args, **kwargs):
        names = kwargs.pop('names', None)

        arr = []
        for sub in self.subdirectories():
            if names and sub.name not in names:
                continue

            resource = Resource(self, uri=sub.as_uri())
            columns = {}

            for path in list(self._p.glob('**/*.[pP][dD][fF]')):
                with open(path.as_posix(), 'rb') as f:
                    try:
                        info = PdfFileReader(f).getDocumentInfo()
                    except PdfReadError:
                        continue

                for k in info.keys():
                    k = k[1:]
                    if k in META_FIELD:
                        continue
                    if k in columns:
                        columns[k] += 1
                    else:
                        columns[k] = 1
            for column, count in columns.items():
                resource.add_column(column, count=count)
            arr.append(resource)

        return arr

    def get_collection(self, *args, **kwargs):
        raise NotImplementedError()


class IndexProfile(AbstractIndexProfile):

    META_FIELD = ('Author', 'CreationDate', 'Creator', 'Keywords',
                  'ModDate', 'Producer', 'Subject', 'Title')

    def __init__(self, name, resource):
        super().__init__(name, resource)

    def _format(fun):

        @wraps(fun)
        def wrapper(self, *args, **kwargs):
            for record in fun(self, *args, **kwargs):

                properties, _backuped = {}, {}
                for k, v in record['properties'].items():
                    prop = self.get_property(k)
                    if prop:
                        if prop.name.startswith('attachment'):
                            continue
                        if prop.rejected:
                            _backuped[prop.name] = v
                        else:
                            properties[prop.alias or prop.name] = v

                yield {
                    '_backup': _backuped,
                    '_md5': digest_binary(record['raw'].encode('utf-8')),
                    'lineage': {
                        'filename': record['filename'],
                        'resource': {
                            'name': self.resource.name},
                        'source': {
                            'protocol': self.resource.source.protocol,
                            'uri': self.resource.source.uri}},
                    'properties': properties,
                    '_raw': record['raw']}

        return wrapper

    @_format
    def get_collection(self, *args, **kwargs):
        yield from self.resource.get_collection()

    def generate_elastic_mapping(self):

        props = {'attachment': {'properties': {}}}
        for p in self.iter_properties():
            if p.name == 'attachment/author':
                props['attachment']['properties']['author'] = fetch_mapping(p)
            elif p.name == 'attachment/content':
                props['attachment']['properties']['content'] = {
                    'analyzer': p.analyzer,
                    'boost': p.weight,
                    # 'eager_global_ordinals'
                    # 'fielddata'
                    # 'fielddata_frequency_filter'
                    'fields': {
                        'keyword': {
                            'index': False,
                            'store': False,
                            'type': 'keyword'}},
                    'index': True,
                    'index_options': 'offsets',
                    'norms': True,
                    'position_increment_gap': 100,
                    'store': False,
                    'search_analyzer': p.search_analyzer,
                    # 'search_quote_analyzer'
                    'similarity': 'classic',
                    'term_vector': 'yes',
                    'type': 'text'}
            elif p.name == 'attachment/content_length':
                props['attachment']['properties']['content_length'] = fetch_mapping(p)
            elif p.name == 'attachment/content_type':
                props['attachment']['properties']['content_type'] = fetch_mapping(p)
            elif p.name == 'attachment/date':
                props['attachment']['properties']['date'] = fetch_mapping(p)
            elif p.name == 'attachment/keywords':
                props['attachment']['properties']['keywords'] = fetch_mapping(p)
            elif p.name == 'attachment/author':
                props['attachment']['properties']['content_length'] = fetch_mapping(p)
            elif p.name == 'attachment/language':
                props['attachment']['properties']['language'] = fetch_mapping(p)
            elif p.name == 'attachment/title':
                props['attachment']['properties']['title'] = fetch_mapping(p)
            elif not p.rejected:
                print(p.name)
                props[p.alias or p.name] = fetch_mapping(p)

        return clean_my_obj({
            self.name: {
                'properties': {
                    'lineage': {
                        'properties': {
                            'filename': not_searchable('keyword'),
                            'resource': {
                                'properties': {
                                    'name': not_searchable('keyword')}},
                            'source': {
                                'properties': {
                                    'protocol': not_searchable('keyword'),
                                    'uri': not_searchable('keyword')}}}},
                    'properties': {'properties': props}}}})
