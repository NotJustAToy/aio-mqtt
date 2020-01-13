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

import enum

__all__ = (
    'ProtocolVersion',
    'PacketType',
    'QOSLevel',
    'ConnectReturnCode',
    'SubscribeReturnCode',
)


class ProtocolVersion(enum.IntEnum):
    MQTTv311 = 4


class PacketType(enum.IntEnum):
    CONNECT = 0x10
    CONNACK = 0x20
    PUBLISH = 0x30
    PUBACK = 0x40
    PUBREC = 0x50
    PUBREL = 0x60
    PUBCOMP = 0x70
    SUBSCRIBE = 0x80
    SUBACK = 0x90
    UNSUBSCRIBE = 0xA0
    UNSUBACK = 0xB0
    PINGREQ = 0xC0
    PINGRESP = 0xD0
    DISCONNECT = 0xE0


class QOSLevel(enum.IntEnum):
    QOS_0 = 0x00
    QOS_1 = 0x01
    QOS_2 = 0x02


class ConnectReturnCode(enum.IntEnum):
    CONNECTION_ACCEPTED = 0x00
    UNACCEPTABLE_PROTOCOL_VERSION = 0x01
    IDENTIFIER_REJECTED = 0x02
    SERVER_UNAVAILABLE = 0x03
    BAD_USERNAME_PASSWORD = 0x04
    NOT_AUTHORIZED = 0x05


class SubscribeReturnCode(enum.IntEnum):
    SUCCESS_MAXIMUM_QOS_0 = 0x00
    SUCCESS_MAXIMUM_QOS_1 = 0x01
    SUCCESS_MAXIMUM_QOS_2 = 0x02
    FAILURE = 0x80
