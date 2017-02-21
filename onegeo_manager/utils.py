def clean_my_dict(d):
    """Supprimer les paires de clé-valeur nulles d'un 'dict'.

    :param d: Un 'dict' sale.
    :return: Un 'dict' propre.
    """
    if not isinstance(d, dict):
        raise TypeError('Argument should be an instance of dict')
    return dict((k, clean_my_dict(v)) for k, v in d.items() if v is not None)


class StaticClass(type):

    def __call__(cls):
        raise TypeError('\'{0}\' static class is not callable.'.format(cls.__qualname__))


class Singleton(type):

    __instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__instances:
            cls.__instances[cls] = super().__call__(*args, **kwargs)
        # else:
        #     cls._instances[cls].__init__(*args, **kwargs)
        return cls.__instances[cls]


class XMLtoObj:

    __obj = {}     # L'objet à retourner
    __cur = None   # Tag de l'élément courant
    __attr = None  # Attribut(s) de l'élément courant
    __txt = None   # Contenu textuel de l'élément courant
    __pos = 0      # Position dans l'arbre
    __path = {}    # Chemin vers l'élément courant

    def __init__(self, attrib_tag='@', text_tag='text'):

        # Définit les options de marquage des éléments XML
        # dans l'objet à retourner

        self.attrib_tag = attrib_tag
        self.text_tag = text_tag

    def start(self, tag, attrib):

        def parse_attribute(d, prefix='@'):
            r = {}
            for k, v in d.items():
                k = k.split('}')[-1]
                r['{0}{1}'.format(prefix, k)] = v
            return r

        def recur_n_insert(tree, depth=0):
            # Parcourt et construit l'objet à la volée.

            path = self.__path
            pos = self.__pos
            cur = self.__cur
            attr = self.__attr

            keys = list(tree.keys())

            if depth == pos:
                if cur in iter(keys):
                    if not isinstance(tree[cur], list):
                        tree[cur] = [tree[cur]]
                    if isinstance(tree[cur], list):
                        tree[cur].append(attr)
                else:
                    tree[cur] = attr

            elif depth < pos:
                last_key = keys[-1]
                val = tree[last_key]
                if last_key == path[depth]:
                    if isinstance(val, list):
                        val = val[-1]
                    recur_n_insert(val, depth + 1)

        self.__cur = tag.split('}')[-1]

        self.__attr = {}
        if attrib:
            self.__attr = parse_attribute(attrib, prefix=self.attrib_tag)

        recur_n_insert(self.__obj)

        self.__path.update({self.__pos: self.__cur})
        self.__pos += 1

    def data(self, data):

        def browse_n_update(tree):
            # Parcourt l'objet et met à jour l'élément courant.

            cur = self.__cur
            txt = self.__txt
            for key, subtree in tree.items():
                if key == cur:
                    if not subtree:
                        tree[key] = txt
                        break
                    if isinstance(subtree, list):
                        subtree[-1] = txt
                    if isinstance(subtree, dict):
                        subtree[self.text_tag] = txt
                if isinstance(subtree, list):
                    subtree = subtree[-1]
                if isinstance(subtree, dict):
                    browse_n_update(subtree)

        self.__txt = data.strip() or None
        if self.__txt:
            browse_n_update(self.__obj)

    def end(self, tag):
        self.__pos -= 1
        # if not tag == self.__path[self.__pos]:
        #     raise ValueError(
        #                 'XML2Obj parsing error',
        #                 'Start tag and end tag are different: '
        #                 "'{0}'<>'{1}'".format(self.__path[self.__pos], tag))
        del self.__path[self.__pos]

    def close(self):
        return self.__obj
