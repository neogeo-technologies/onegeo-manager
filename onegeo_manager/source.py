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


from abc import ABCMeta
from abc import abstractmethod
from importlib import import_module
import inspect
from onegeo_manager.exception import ProtocolNotFoundError
import os.path


__all__ = ['Source']


class AbstractSource(metaclass=ABCMeta):

    def __init__(self, uri):

        self.protocol = os.path.basename(
            inspect.getmodule(inspect.stack()[1][0]).__file__[:-3])
        self.uri = uri

    @abstractmethod
    def get_resources(self, *args, **kwargs):
        raise NotImplementedError(
            "This is an abstract method. You can't do anything with it.")

    # @abstractmethod
    # def get_collection(self, *args, **kwargs):
    #     raise NotImplementedError(
    #         "This is an abstract method. You can't do anything with it.")


class Source(object):

    def __new__(self, uri, protocol, **kwargs):

        try:
            ext = import_module(
                'onegeo_manager.protocol.{0}'.format(protocol), __name__)
        except ModuleNotFoundError as e:
            raise ProtocolNotFoundError

        self = object.__new__(ext.Source)
        self.__init__(uri, **kwargs)
        return self
