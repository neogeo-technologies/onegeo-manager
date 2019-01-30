# Copyright (c) 2017-2019 Neogeo-Technologies.
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


class GenericException(Exception):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return str(self.args or self.__class__.__name__)


class OGCExceptionReport(GenericException):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class OGCDocumentError(GenericException):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class UnexpectedError(GenericException):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class ProtocolNotFoundError(GenericException):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class NotYetImplemented(GenericException):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
