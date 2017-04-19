from abc import ABCMeta, abstractmethod
from base64 import b64encode
from pathlib import Path
from re import search

from PyPDF2 import PdfFileReader

from .method import CswMethod, GeonetMethod, WfsMethod
from .resource import Resource
from .utils import obj_browser, ows_response_converter


__all__ = ['Source']


class AbstractSource(metaclass=ABCMeta):

    def __init__(self, uri, name, mode):
        self.uri = uri
        self.mode = mode

        s = search('^[a-z0-9_]{2,100}$', name)
        if not s:
            raise ValueError("Malformed value for 'name'.")
        self.name = name

    @abstractmethod
    def get_resources(self, *args, **kwargs):
        raise NotImplementedError("This is an abstract method. "
                                  "You can't do anything with it.")

    @abstractmethod
    def get_collection(self, *args, **kwargs):
        raise NotImplementedError("This is an abstract method. "
                                  "You can't do anything with it.")


class CswSource(AbstractSource):

    def __init__(self, url, name, mode):
        super().__init__(url, name, mode)

        self.capabilities = self.__get_capabilities()['Capabilities']

    def get_resources(self):
        return [Resource(self, 'dataset'),
                Resource(self, 'nonGeographicDataset'),
                Resource(self, 'series'),
                Resource(self, 'service')]

    def get_collection(self, resource_name, count=100):

        params = {'version': self.capabilities['@version']}

        if params['version'] != '2.0.2':
            raise NotImplemented(
                    'Version {0} not implemented.'.format(params['VERSION']))

        params.update({
            'constraint': "type LIKE '{0}'".format(resource_name),
            'constraint_language_version': '1.0.0',
            'constraintlanguage': 'CQL_TEXT',
            'elementsetname': 'full',
            'maxrecords': count,
            'outputschema': 'http://www.isotc211.org/2005/gmd',
            'resulttype': 'results',
            'startposition': 1,
            'typenames': 'csw:Record'})  # gmd:MD_Metadata, csw:Record

        while True:
            data = self.__get_records(**params)['GetRecordsResponse']['SearchResults']['Record']
            yield from data
            if len(data) < count:
                break
            params['startposition'] += count

    @ows_response_converter
    def __get_capabilities(self, **params):
        return CswMethod.get_capabilities(self.uri, **params)

    @ows_response_converter
    def __get_records(self, **params):
        return CswMethod.get_records(self.uri, **params)


class GeonetSource(AbstractSource):

    def __init__(self, url, name, mode):
        super().__init__(url, name, mode)

        params = {'fast': 'true', 'from': 0, 'to': 0}
        self.summary = self.__search(**params)['response']['summary']

    def get_resources(self):
        resources = []
        for entry in self.summary['types']['type']:
            resource = Resource(self, entry['@name'])
            resource.add_column('title', column_type='keyword')
            resource.add_column('abstract', column_type='text')
            resource.add_column('keyword', column_type='keyword')
            resources.append(resource)
        return resources

    def get_collection(self, resource_name, count=100):

        params = {'fast': 'false', 'from': 1,
                  'to': count, 'type': resource_name}

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


class PdfSource(AbstractSource):

    META_FIELD = ('Author', 'CreationDate', 'Creator', 'Keywords',
                  'ModDate', 'Producer', 'Subject', 'Title')

    def __init__(self, path, name, mode):

        self.__p = Path(path.startswith('file://') and path[7:] or path)
        if not self.__p.exists():
            raise ConnectionError('The given path does not exist.')

        super().__init__(self.__p.as_uri(), name, mode)

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
                   'filename': path.name,
                   'properties': format(dict(PdfFileReader(f).getDocumentInfo()))}


class WfsSource(AbstractSource):

    def __init__(self, url, name, mode):
        super().__init__(url, name, mode)

        self.capabilities = self.__get_capabilities()['WFS_Capabilities']

        self.title = obj_browser(
                self.capabilities, 'ServiceIdentification', 'Title')

        self.abstract = obj_browser(
                self.capabilities, 'ServiceIdentification', 'Abstract')

        self.metadata_url = ''

    def _retreive_ft_meta(self, ft_name):
        for f in iter(self.capabilities['FeatureTypeList']['FeatureType']):
            if f['Name'].split(':')[-1] == ft_name:
                return f
        raise ValueError('{0} not found.'.format(ft_name))

    def get_resources(self):

        desc = self.__describe_feature_type(
                                    version=self.capabilities['@version'])

        resources = []
        for elt in iter([(m['@name'], m['@type'].split(':')[-1])
                                        for m in desc['schema']['element']]):

            resource = Resource(self, elt[0])

            ct = None
            for complex_type in iter(desc['schema']['complexType']):
                if complex_type['@name'] == elt[1]:
                    ct = complex_type
                    break

            for e in ct['complexContent']['extension']['sequence']['element']:
                n = '@name' in e and str(e['@name']) or None
                t = '@type' in e and str(e['@type']).split(':')[-1] or None
                o = ('@minOccurs' in e and int(e['@minOccurs']) or 0,
                     '@maxOccurs' in e and int(e['@maxOccurs']) or 1)
                resource.add_column(n, column_type=t, occurs=o)

            resources.append(resource)
        return resources

    def get_collection(self, resource_name, count=100):
        """

        :param resource_name: Le nom du type d'objets à retourner.
        :param count: Le pas de pagination du GetFeature (opt).
        :return: Un générateur contenant des GeoJSON.
        """

        capacity = self._retreive_ft_meta(resource_name)

        params = {'version': self.capabilities['@version']}

        if params['version'] != '2.0.0':
            raise NotImplemented(
                    'Version {0} not implemented.'.format(params['VERSION']))

        crs_str = ','.join(capacity['OtherCRS'] + [capacity['DefaultCRS']])
        format_str = ','.join(capacity['OutputFormats']['Format'])

        testing = {
            'srsname': {
                'pattern': '((^|((\w*\:+)+))4326)',
                'string': crs_str},
            'outputformat': {
                'pattern': '((text|application)\/json\;?\s?subtype\=geojson)',
                'string': format_str}}

        for k, v in testing.items():
            s = search(v['pattern'], v['string'])
            if not s:
                raise ValueError('TODO')  # TODO
            params[k] = s.group(0)

        params.update({'typenames': resource_name,
                       'startindex': 0,
                       'count': count})

        # C'est très moche mais c'est pour contourner un bug(?) de 'aiohttp'
        # params['OUTPUTFORMAT'] = 'geojson'

        while True:
            # Boucle sur le GetFeature tant que tous
            # les objets ne sont pas recupérés.
            data = self.__get_feature(**params)['features']
            yield from data
            if len(data) < count:
                break
            params['startindex'] += count

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
        self.__init__(uri, name, mode)
        return self
