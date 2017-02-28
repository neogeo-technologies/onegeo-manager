from abc import ABCMeta, abstractmethod
from base64 import b64encode
from pathlib import Path
from re import search

from PyPDF2 import PdfFileReader

from .wfs import WfsMethod
from .type import FeatureType, PdfType


__all__ = ['PdfSource', 'WfsSource']


class GenericSource(metaclass=ABCMeta):

    MODE = ('pdf', 'wfs')

    def __init__(self, uri, name, mode):
        self.uri = uri

        s = search('^[a-z0-9_]{3,30}$', name)
        if not s:
            raise ValueError("Malformed value fo 'name'.")
        self.name = name

        if not self.authorized_mode(mode):
            raise ValueError("Mode value '{0}' not authorized.".format(mode))
        self.mode = mode

    def authorized_mode(self, val):
        return val in self.MODE

    @abstractmethod
    def get_types(self, *args, **kwargs):
        raise NotImplementedError("This is an abstract method. "
                                  "You can't do anything with it.")

    @abstractmethod
    def get_collection(self, *args, **kwargs):
        raise NotImplementedError("This is an abstract method. "
                                  "You can't do anything with it.")


class PdfSource(GenericSource):

    def __init__(self, path, name, mode):

        self.__p = Path(path.startswith('file://') and path[7:] or path)
        if not self.__p.exists():
            raise ConnectionError('The given path does not exist.')

        super().__init__(self.__p.as_uri(), name, mode)

    def _iter_pdf_path(self):
        return iter(list(self.__p.glob('**/*.pdf')))

    def _iter_dir_path(self):
        subdirs = [sub for sub in self.__p.iterdir() if sub.is_dir()]
        if subdirs:
            return iter(subdirs)
        return iter([self.__p])

    def get_types(self):

        arr = []
        for d in self._iter_dir_path():
            t = PdfType(d.name)
            for p in self._iter_pdf_path():
                pdf = PdfFileReader(open(p.as_posix(), 'rb'))
                for k, _ in pdf.getDocumentInfo().items():
                    if k not in t.iter_column_name():
                        t.add_column(k)
            arr.append(t)
        return arr

    def get_collection(self):
        for f in self._iter_pdf_path():
            yield {'data': b64encode(open(f, 'rb').read()),
                   'meta': dict(PdfFileReader(
                                    open(f, 'rb')).getDocumentInfo().items())}


class WfsSource(GenericSource):

    def __init__(self, url, name, mode):
        super().__init__(url, name, mode)

        self.capabilities = self.__get_capabilities()['WFS_Capabilities']

    def get_types(self):

        desc = self.__describe_feature_type(
                                    version=self.capabilities['@version'])

        types = []
        for e in iter([(m['@name'], m['@type'].split(':')[-1])
                       for m in desc['schema']['element']]):

            ft = FeatureType(e[0])

            t = None
            for complex_type in iter(desc['schema']['complexType']):
                if complex_type['@name'] == e[1]:
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
        features = []

        def find_ft_meta(ft_name):
            for f in iter(self.capabilities['FeatureTypeList']['FeatureType']):
                if f['Name'].split(':')[-1] == ft_name:
                    return f
            raise ValueError('{0} not found.'.format(ft_name))

        capacity = find_ft_meta(typename)

        params = {'VERSION': self.capabilities['@version']}

        if params['VERSION'] == '2.0.0':

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

            params.update({'TYPENAMES': typename, 'STARTINDEX': 0, 'COUNT': count})

            # C'est très moche mais c'est pour contourner un bug(?) de 'aiohttp'
            params['OUTPUTFORMAT'] = 'geojson'

            while True:
                # Boucle sur le GetFeature tant que tous
                # les objets ne sont pas recupérés.
                res = self.__get_feature(**params)
                features += res['features']
                if len(res['features']) < count:
                    break
                params['STARTINDEX'] += count
                yield from features

        else:
            raise NotImplemented(
                    'Version {0} not implemented.'.format(params['VERSION']))

    def __get_capabilities(self, **params):
        return WfsMethod.get_capabilities(self.uri, **params)

    def __describe_feature_type(self, **params):
        return WfsMethod.describe_feature_type(self.uri, **params)

    def __get_feature(self, **params):
        return WfsMethod.get_feature(self.uri, **params)
