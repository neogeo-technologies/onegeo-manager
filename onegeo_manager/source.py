from abc import ABCMeta, abstractmethod
from base64 import b64encode
from pathlib import Path
from re import search

from PyPDF2 import PdfFileReader

from .ogc import CswMethod, GeonetMethod, WfsMethod
from .type import Type
from .utils import from_camel_was_born_snake, ows_response_converter


__all__ = ['Source']


class GenericSource(metaclass=ABCMeta):

    def __init__(self, uri, name):
        self.uri = uri

        s = search('^[a-z0-9_]{2,30}$', name)
        if not s:
            raise ValueError("Malformed value for 'name'.")
        self.name = name

    @abstractmethod
    def get_types(self, *args, **kwargs):
        raise NotImplementedError("This is an abstract method. "
                                  "You can't do anything with it.")

    @abstractmethod
    def get_collection(self, *args, **kwargs):
        raise NotImplementedError("This is an abstract method. "
                                  "You can't do anything with it.")


class CswSource(GenericSource):

    def __init__(self, url, name):
        super().__init__(url, name)

        self.capabilities = self.__get_capabilities()['Capabilities']

    def get_types(self):
        return [Type(self, 'dataset'),
                Type(self, 'nonGeographicDataset'),
                Type(self, 'series'),
                Type(self, 'service')]

    def get_collection(self, typename='dataset', count=100):

        params = {'VERSION': self.capabilities['@version']}

        if params['VERSION'] != '2.0.2':
            raise NotImplemented(
                    'Version {0} not implemented.'.format(params['VERSION']))

        params.update({
            'CONSTRAINT': "type LIKE '{0}'".format(typename),
            'CONSTRAINT_LANGUAGE_VERSION': '1.0.0',
            'CONSTRAINTLANGUAGE': 'CQL_TEXT',
            'ELEMENTSETNAME': 'full',
            'MAXRECORDS': count,
            'OUTPUTSCHEMA': 'http://www.isotc211.org/2005/gmd',
            'RESULTTYPE': 'results',
            'STARTPOSITION': 1,
            'TYPENAMES': 'csw:Record'}) # gmd:MD_Metadata, csw:Record

        while True:
            data = self.__get_records(**params)['GetRecordsResponse']['SearchResults']['Record']
            yield from data
            if len(data) < count:
                break
            params['STARTPOSITION'] += count

    @ows_response_converter
    def __get_capabilities(self, **params):
        return CswMethod.get_capabilities(self.uri, **params)

    @ows_response_converter
    def __get_records(self, **params):
        return CswMethod.get_records(self.uri, **params)


class GeonetSource(GenericSource):

    def __init__(self, url, name):
        super().__init__(url, name)

        params = {'fast': 'true', 'from': 0, 'to': 0}
        self.summary = self.__search(**params)['response']['summary']

    def get_types(self):
        types = []
        for entry in self.summary['types']['type']:
            type = Type(self, entry['@name'])
            type.add_column('identifier', column_type='keyword')
            type.add_column('title', column_type='keyword')
            type.add_column('abstract', column_type='text')
            type.add_column('keyword', column_type='keyword')
            type.add_column('subject', column_type='keyword')
            type.add_column('info', column_type='object', occurs=(1, 1))
            types.append(type)
        return types

    def get_collection(self, typename, count=100):

        params = {'fast': 'false', 'from': 1, 'to': count, 'type': typename}

        while True:
            data = self.__search(**params)['response']['metadata']
            yield from data
            if len(data) < count:
                break
            params['from'] += count
            params['to'] += count

    @ows_response_converter
    def __search(self, **params):
        return GeonetMethod.search(self.uri, **params)


class PdfSource(GenericSource):

    META_FIELD = ('Author', 'CreationDate', 'Creator', 'Keywords',
                  'ModDate', 'Producer', 'Subject', 'Title')

    def __init__(self, path, name):

        self.__p = Path(path.startswith('file://') and path[7:] or path)
        if not self.__p.exists():
            raise ConnectionError('The given path does not exist.')

        super().__init__(self.__p.as_uri(), name)

    def _iter_pdf_path(self, subdir_name):
        return iter(list(self.__p.glob('{0}/**/*.pdf'.format(subdir_name))))

    def _iter_dir_path(self):
        subdirs = [sub for sub in self.__p.iterdir() if sub.is_dir()]
        if subdirs:
            return iter(subdirs)
        return iter([self.__p])

    def get_types(self):

        arr = []
        for subdir in self._iter_dir_path():
            columns = {}
            t = Type(self, from_camel_was_born_snake(subdir.name))
            for p in self._iter_pdf_path(subdir.name):
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
                t.add_column(column, count=count)
            arr.append(t)

        return arr

    def get_collection(self, type_name):

        def format(meta):
            copy = {}
            for k, v in meta.items():
                k = k[1:]
                if k in self.META_FIELD:
                    continue
                copy[k] = v
            return copy

        target = None
        for subdir in self._iter_dir_path():
            if subdir.name == type_name:
                target = subdir
                break

        if not target:
            raise ValueError('{0} not found.'.format(type_name))

        for path in self._iter_pdf_path(target.name):
            f = open(path.as_posix(), 'rb')
            yield {'data': b64encode(f.read()).decode('utf-8'),
                   'filename': path.name,
                   'meta': format(dict(PdfFileReader(f).getDocumentInfo()))}


class WfsSource(GenericSource):

    def __init__(self, url, name):
        super().__init__(url, name)

        self.capabilities = self.__get_capabilities()['WFS_Capabilities']

    def get_types(self):

        desc = self.__describe_feature_type(
                                    version=self.capabilities['@version'])

        types = []
        for elt in iter([(m['@name'], m['@type'].split(':')[-1])
                       for m in desc['schema']['element']]):

            ft = Type(self, elt[0])

            t = None
            for complex_type in iter(desc['schema']['complexType']):
                if complex_type['@name'] == elt[1]:
                    t = complex_type
                    break

            for e in t['complexContent']['extension']['sequence']['element']:
                n = '@name' in e and str(e['@name']) or None
                t = '@type' in e and str(e['@type']).split(':')[-1] or None
                o = ('@minOccurs' in e and int(e['@minOccurs']) or 0,
                     '@maxOccurs' in e and int(e['@maxOccurs']) or 1)

                if n in ['msGeometry', 'geometry']:  # TODO
                    ft.set_geometry_column(t)
                else:
                    ft.add_column(n, column_type=t, occurs=o)

            types.append(ft)
        return types

    def get_collection(self, typename, count=100):
        """

        :param typename: Le nom du type d'objets à retourner.
        :param count: Le pas de pagination du GetFeature (opt).
        :return: Un générateur contenant des GeoJSON.
        """

        def retreive_ft_meta(ft_name):
            for f in iter(self.capabilities['FeatureTypeList']['FeatureType']):
                if f['Name'].split(':')[-1] == ft_name:
                    return f
            raise ValueError('{0} not found.'.format(ft_name))

        capacity = retreive_ft_meta(typename)

        params = {'VERSION': self.capabilities['@version']}

        if params['VERSION'] != '2.0.0':
            raise NotImplemented(
                    'Version {0} not implemented.'.format(params['VERSION']))

        crs_str = ','.join(capacity['OtherCRS'] + [capacity['DefaultCRS']])
        format_str = ','.join(capacity['OutputFormats']['Format'])

        testing = {
            'SRSNAME': {
                'pattern': '((^|((\w*\:+)+))4326)',
                'string': crs_str},
            'OUTPUTFORMAT': {
                'pattern': '((text|application)\/json\;?\s?subtype\=geojson)',
                'string': format_str}}

        for k, v in testing.items():
            s = search(v['pattern'], v['string'])
            if not s:
                raise ValueError('TODO')  # TODO
            params[k] = s.group(0)

        params.update({'TYPENAMES': typename,
                       'STARTINDEX': 0,
                       'COUNT': count})

        # C'est très moche mais c'est pour contourner un bug(?) de 'aiohttp'
        params['OUTPUTFORMAT'] = 'geojson'

        while True:
            # Boucle sur le GetFeature tant que tous
            # les objets ne sont pas recupérés.
            data = self.__get_feature(**params)['features']
            yield from data
            if len(data) < count:
                break
            params['STARTINDEX'] += count

    @ows_response_converter
    def __get_capabilities(self, **params):
        return WfsMethod.get_capabilities(self.uri, **params)

    @ows_response_converter
    def __describe_feature_type(self, **params):
        return WfsMethod.describe_feature_type(self.uri, **params)

    @ows_response_converter
    def __get_feature(self, **params):
        return WfsMethod.get_feature(self.uri, **params)


class Source:

    def __new__(cls, uri, name, mode):

        modes = {'csw': CswSource,
                 'geonet': GeonetSource,
                 'pdf': PdfSource,
                 'wfs': WfsSource}

        cls = modes.get(mode, None)
        if not cls:
            raise ValueError('Unrecognized mode.')

        self = object.__new__(cls)
        self.__init__(uri, name)
        return self
