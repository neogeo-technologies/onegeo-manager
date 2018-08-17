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
import gc
from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.index_profile import fetch_mapping
# from onegeo_manager.index_profile import not_searchable
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import clean_my_obj
from onegeo_manager.utils import digest_binary
from pathlib import Path
from PyPDF2 import PdfFileReader
from PyPDF2.utils import PdfReadError
import re


__description__ = 'PDF Store'


class Resource(AbstractResource):

    def __init__(self, source, uri=None):
        self._uri = uri
        self._p = Path(uri[7:])
        super().__init__(source, name=self._p.name)

        self.title = self._p.name

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
            doc = {
                'raw': None,
                'filename': filename,
                'properties': {}}

            gc.collect(generation=2)

            info = {}
            with open(path.as_posix(), 'rb') as f:
                try:
                    info = PdfFileReader(f).getDocumentInfo()
                except Exception:
                    pass

            with open(path.as_posix(), 'rb') as f:
                doc['raw'] = b64encode(f.read()).decode('utf-8')

            for k, v in info.items():
                k = k.startswith('/') and k[1:] or k
                rule = dict(
                    (c['name'], c) for c in self.columns)[k].get('rule')
                if rule:
                    try:
                        matched = re.match(rule, v)
                    except Exception:
                        pass
                    else:
                        if matched:
                            for x in matched.groupdict().keys():
                                doc['properties'][x] = matched.groupdict().get(x)
                else:
                    doc['properties'][k] = v
            yield doc


class Source(AbstractSource):

    def __init__(self, uri):
        self._p = Path(uri.startswith('file://') and uri[7:] or uri)
        if not self._p.exists():
            raise ConnectionError('The given path does not exist.')
        super().__init__(uri)

    def subdirectories(self):
        return [sub for sub in self._p.iterdir() if sub.is_dir()] or [self._p]

    def get_resource(self, name, **kwargs):
        force_columns = kwargs.pop('force_columns', {})

        sub = (self._p / name)
        if not sub.exists():
            raise ConnectionError('The asked resource does not exist.')

        resource = Resource(self, uri=sub.as_uri())

        if force_columns:
            for col in force_columns:
                name = col.pop('name')
                column_type = col.pop('type', None)
                resource.add_column(
                    name, column_type=column_type, **col)
        else:
            columns = {}
            for p in list(sub.glob('**/*.[pP][dD][fF]')):
                with open(p.as_posix(), 'rb') as f:
                    try:
                        info = PdfFileReader(f).getDocumentInfo()
                    except PdfReadError:
                        continue
                    # else
                    for k in info.keys():
                        k = k.startswith('/') and k[1:] or k
                        if k in columns:
                            columns[k] += 1
                        else:
                            columns[k] = 1

            for name, count in columns.items():
                resource.add_column(name, count=count)
            resource.add_column('Content', column_type='text', occurs=(1, 1))

        return resource

    def get_resources(self, *args, **kwargs):
        names = kwargs.pop('names', [s.name for s in self.subdirectories()])
        force_columns = kwargs.pop('columns', {})

        resources = []
        for name in names:
            resources.append(self.get_resource(name, force_columns=force_columns))
        return resources

    def get_collection(self, *args, **kwargs):
        raise NotImplementedError()


class IndexProfile(AbstractIndexProfile):

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

        properties = {}
        attachment_properties = {}
        for p in self.iter_properties():
            if p.name == 'Content':
                attachment_properties['content'] = {
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
            elif not p.rejected:
                properties[p.alias or p.name] = fetch_mapping(p)

        return clean_my_obj({
            self.name: {
                'properties': {
                    'attachment': {
                        'properties': attachment_properties},
                    'lineage': {
                        'properties': {
                            'filename': {'type': 'keyword'},
                            'resource': {
                                'properties': {
                                    'name': {'type': 'keyword'}}},
                            'source': {
                                'properties': {
                                    'protocol': {'type': 'keyword'},
                                    'uri': {'type': 'keyword'}}}}},
                    'properties': {
                        'properties': properties}}}})
