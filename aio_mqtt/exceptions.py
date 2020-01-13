# Copyright 2019-2020 Not Just A Toy Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .enums import ConnectReturnCode

__all__ = (
    'Error',
    'ProtocolError',
    'AlreadyConnectedError',
    'ConnectionClosedError',
    'ServerDiedError',
    'ConnectionCloseForcedError',
    'ConnectFailedError',
    'AccessRefusedError',
    'ConnectionLostError',
)


class Error(Exception):
    pass


class ProtocolError(Error):
    pass


class AlreadyConnectedError(Error):
    pass


class ConnectionClosedError(Error):
    pass


class ServerDiedError(Error):
    pass


class ConnectionCloseForcedError(ConnectionClosedError):
    pass


class ConnectionLostError(ConnectionClosedError):
    pass


class ConnectFailedError(Error):
    pass


class AccessRefusedError(Error):

    def __init__(self, message: str, return_code: ConnectReturnCode) -> None:
        self.message = message
        self.return_code = return_code

    def __str__(self) -> str:
        return self.message
