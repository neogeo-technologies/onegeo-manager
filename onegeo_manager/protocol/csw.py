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


from functools import wraps
import itertools
from onegeo_manager.index_profile import AbstractIndexProfile
from onegeo_manager.index_profile import fetch_mapping
from onegeo_manager.index_profile import not_searchable
from onegeo_manager.resource import AbstractResource
from onegeo_manager.source import AbstractSource
from onegeo_manager.utils import clean_my_obj
import operator
from owslib import csw


__description__ = 'OGC:CSW'


class Resource(AbstractResource):

    def __init__(self, source, name=None):
        super().__init__(source, name=name)

        self.title = name

    def authorized_column_type(self, val):
        return val in operator.add(self.COLUMN_TYPE, ['object', 'geo_shape'])

    def get_collection(self, step=10, id_record=[]):
        raise NotImplementedError('TODO')


class Source(AbstractSource):

    OUTPUSCHEMA = {
        'http://www.opengis.net/cat/csw/2.0.2': [
            'nonGeographicDataset', 'service'],
        'http://www.isotc211.org/2005/gmd': ['dataset', 'series']}

    def __init__(self, url, username=None, password=None):
        super().__init__(url)
        self._csw = \
            csw.CatalogueServiceWeb(url, username=username, password=password)
        self.capabilities = self._csw.response

    def get_resources(self, names=[]):
        auth_names = ['dataset', 'nonGeographicDataset', 'series', 'service']
        if names and not any(map(lambda v: v in names, auth_names)):
            raise ValueError('Some given names are not found in this context.')

        resources = []
        for val in names or auth_names:
            resource = Resource(self, val)

            if val in self.OUTPUSCHEMA['http://www.isotc211.org/2005/gmd']:
                # -> MD_Metadata
                columns = (
                    ('abstract', 'text'),
                    ('bbox', 'geo_shape'),
                    ('classification', 'text'),
                    ('contact', 'object'),
                    ('date_publication', 'date'),
                    ('denominators', 'integer'),
                    # ('distance', 'integer'),
                    ('identifier', 'text'),
                    ('keyword', 'object'),
                    ('lineage', 'text'),
                    ('parent_identifier', 'text'),
                    ('resolution', 'object'),
                    ('rights', 'text'),
                    ('schema', 'text'),
                    ('spatial_type', 'text'),
                    ('standard', 'object'),
                    ('title', 'text'),
                    ('topic_category', 'text'),
                    ('type', 'text'),
                    # ('uom', 'text'),
                    ('uris', 'object'),
                    ('use_constraints', 'text'),
                    ('use_limitation', 'text'),
                    ('xml', 'text'))

            if val in self.OUTPUSCHEMA['http://www.opengis.net/cat/csw/2.0.2']:
                # -> CswRecord
                columns = (
                    ('abstract', 'text'),  # dc
                    # ('accessrights', 'text'),  # dct
                    # ('alternative', 'text'),  # dct
                    # ('created', 'text'),  # dct
                    # ('bbox', 'geo_shape'),  # ows
                    # ('bbox_wgs84', 'geo_shape'), # ows
                    ('date', 'date'),  # dc
                    ('identifier', 'text'),  # dc
                    # ('ispartof', 'text'),  # dct
                    # ('issued', 'text'),  # dct
                    # ('license', 'text'),  # dct
                    # ('modified', 'text'),  # dct
                    # ('references', 'object'),  # dct
                    ('relation', 'text'),  # dc
                    ('rights', 'text'),  # dc
                    # ('rightsholder', 'text'),  # dct
                    ('schema', 'text'),
                    # ('spatial', 'text'),  # dct
                    ('source', 'text'),  # dc
                    ('subjects', 'text'),  # dc
                    # ('temporal', 'text'),  # dct
                    ('title', 'text'),  # dc
                    ('type', 'text'),  # dc
                    ('uris', 'object'),  # dc
                    ('xml', 'text'))  # dc

            resource.add_columns(
                tuple({'name': m[0], 'column_type': m[1]} for m in columns))
            resources.append(resource)
        return resources

    def get_collection(self, resource, step=10, id_record=[]):

        outputschema = tuple(
            tuple(k for v in l if v == resource.name)[0]
            for k, l in self.OUTPUSCHEMA.items()
            if resource.name in l)[0]

        params = {
            'cql': "type='{0}'".format(resource.name),
            'esn': 'full',
            'format': 'application/xml',
            'maxrecords': step,
            'outputschema': outputschema,
            'resulttype': 'results',
            'startposition': 0,
            'typenames': 'csw:Record'}

        if len(id_record) > 0:
            params['cql'] += " AND (identifier='{1}')".format(
                resource.name, "' OR identifier='".join(id_record))

        while True:
            self._csw.getrecords2(**params)
            records = list(self._csw.records.values())
            for rec in records:
                data = {}
                if rec.__class__.__name__ == 'MD_Metadata':

                    resolution = []
                    distance = rec.identification.distance
                    uom = rec.identification.uom
                    if len(distance) == len(uom):
                        for i in range(len(distance)):
                            resolution.append({
                                'uom': uom[i], 'distance': distance[i]})

                    contact = []
                    if rec.identification.contact:
                        for m in rec.identification.contact:
                            if m.__class__.__name__ == 'CI_ResponsibleParty':
                                d = {}
                                for k in m.__dict__.keys():
                                    v = getattr(m, k)
                                    if v.__class__.__name__ == 'CI_OnlineResource':
                                        v = v.__dict__
                                    d[k] = v
                                contact.append(d)

                    data.update(**{
                        'abstract': rec.identification.abstract,
                        'bbox': rec.identification.bbox and {
                            'type': 'Polygon',
                            'coordinates': [[
                                [rec.identification.bbox.minx,
                                 rec.identification.bbox.miny],
                                [rec.identification.bbox.maxx,
                                 rec.identification.bbox.miny],
                                [rec.identification.bbox.maxx,
                                 rec.identification.bbox.maxy],
                                [rec.identification.bbox.minx,
                                 rec.identification.bbox.maxy],
                                [rec.identification.bbox.minx,
                                 rec.identification.bbox.miny]]]},
                        'classification': rec.identification.classification,
                        'contact': contact,
                        'date_publication': rec.identification.date and [
                            m.date for m in rec.identification.date
                            if m.__class__.__name__ == 'CI_Date'
                            and m.type == 'publication'],
                        'denominators': rec.identification.denominators,
                        'identifier': rec.identifier,
                        'keyword': [y for x in [
                            m.keywords for m in rec.identification.keywords2
                            if m.__class__.__name__ == 'MD_Keywords']
                            for y in x],
                        'lineage': rec.dataquality.lineage,
                        'parent_identifier': rec.parentidentifier,
                        'resolution': resolution,
                        'rights': list(itertools.chain(
                            rec.identification.accessconstraints,
                            rec.identification.securityconstraints,
                            rec.identification.otherconstraints)),
                        'spatial_type':
                            rec.identification.spatialrepresentationtype,
                        'standard': {
                            'name': rec.stdname,
                            'version': rec.stdver},
                        'title': rec.identification.title,
                        'type': rec.hierarchy,
                        'topic_category': rec.identification.topiccategory,
                        'use_constraints': rec.identification.useconstraints,
                        'use_limitation': rec.identification.uselimitation,
                        'uris': rec.distribution.online and [
                            m.__dict__ for m in rec.distribution.online
                            if m.__class__.__name__ == 'CI_OnlineResource'],
                        'xml': rec.xml.decode('utf-8')})

                if rec.__class__.__name__ == 'CswRecord':
                    for col in resource.iter_columns():
                        try:
                            attr = getattr(rec, col['name'])
                        except AttributeError:
                            data[col['name']] = None
                            continue
                        if col['name'] == 'bbox_wgs84' \
                                and col['type'] == 'geo_shape' and attr:
                            attr = {
                                'type': 'Polygon',
                                'coordinates': [[
                                    [attr.minx, attr.miny],
                                    [attr.maxx, attr.miny],
                                    [attr.maxx, attr.maxy],
                                    [attr.minx, attr.maxy],
                                    [attr.minx, attr.miny]]]}

                        data[col['name']] = \
                            isinstance(attr, bytes) and attr.decode() or attr

                data.update(schema=outputschema)

                yield clean_my_obj(data, fading=False)

            if len(records) < step:
                break
            params['startposition'] += step


class IndexProfile(AbstractIndexProfile):

    def __init__(self, name, resource):
        super().__init__(name, resource)

    def authorized_column_type(self, val):
        return val in operator.add(self.COLUMN_TYPE, ['object', 'geo_shape'])

    def _format(fun):

        @wraps(fun)
        def wrapper(self, *args, **kwargs):

            for record in fun(self, *args, **kwargs):

                properties, _backuped = {}, {}
                for k, v in record.items():
                    prop = self.get_property(k)
                    if prop.rejected:
                        _backuped[prop.name] = v
                    else:
                        properties[prop.alias or prop.name] = v

                xml = 'xml' in record and record.pop('xml') or None
                uris = 'uris' in record and record.pop('uris') or None
                yield {
                    '_backup': _backuped,
                    '_md5': None,
                    'lineage': {
                        'resource': {
                            'name': self.resource.name},
                        'source': {
                            'protocol': self.resource.source.protocol,
                            'uri': self.resource.source.uri}},
                    'properties': properties,
                    'uri': uris,
                    'xml': isinstance(xml, bytes) and xml.decode('utf-8') or xml}

        return wrapper

    @_format
    def get_collection(self, **opts):
        return self.resource.source.get_collection(self.resource, **opts)

    def generate_elastic_mapping(self):

        props = {}
        for p in self.iter_properties(ignore=['xml', 'uris']):
            if not p.rejected:
                props[p.alias or p.name] = fetch_mapping(p)

        return clean_my_obj({
            self.name: {
                'properties': {
                    'lineage': {
                        'properties': {
                            'resource': {
                                'properties': {
                                    'name': not_searchable('keyword')}},
                            'source': {
                                'properties': {
                                    'protocol': not_searchable('keyword'),
                                    'uri': not_searchable('keyword')}}}},
                    'properties': {'properties': props},
                    'uri': {
                        'properties': {
                            'protocol': not_searchable('keyword'),
                            'name': not_searchable('text'),
                            'description': not_searchable('text'),
                            'url': not_searchable('keyword')}},
                    'xml': not_searchable('text')}}})
