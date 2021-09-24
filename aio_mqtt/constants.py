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

import re

from .enums import ConnectReturnCode

__all__ = (
    'CONNECT_RETURN_CODE_DESCRIPTIONS',
    'MAXIMUM_PAYLOAD_SIZE',
    'MAXIMUM_TOPIC_LENGTH',
    'MAXIMUM_PACKET_ID',
    'TOPIC_FILTER_REGEX',
    'TOPIC_NAME_REGEX',
)


CONNECT_RETURN_CODE_DESCRIPTIONS = {
    ConnectReturnCode.CONNECTION_ACCEPTED:
        "Connection accepted",
    ConnectReturnCode.UNACCEPTABLE_PROTOCOL_VERSION:
        "The server does not support the level of the MQTT protocol requested by the client",
    ConnectReturnCode.IDENTIFIER_REJECTED:
        "The client identifier is correct UTF-8 but not allowed by the server",
    ConnectReturnCode.SERVER_UNAVAILABLE:
        "The network connection has been made but the MQTT service is unavailable",
    ConnectReturnCode.BAD_USERNAME_PASSWORD:
        "The data in the user name or password is malformed",
    ConnectReturnCode.NOT_AUTHORIZED:
        "The client is not authorized to connect"
}

MAXIMUM_PAYLOAD_SIZE: int = 268435455
MAXIMUM_TOPIC_LENGTH: int = 65535
MAXIMUM_PACKET_ID: int = 65536

TOPIC_FILTER_REGEX = re.compile(r'^/?(\+|[\w\.-]+)(/(\+|[\w\.-]+))*(/#)?$')
TOPIC_NAME_REGEX = re.compile(r'^/?[\w\.-]+(/[\w\.-]+)*$')
