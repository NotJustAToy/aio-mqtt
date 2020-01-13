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

import asyncio as aio
import dataclasses
import typing as ty

__all__ = (
    'ForbiddenReadingError',
    'LimitedStreamReader',
    'TopicMatcher',
)


class ForbiddenReadingError(Exception):
    pass


class LimitedStreamReader:

    __slots__ = ('reader', 'remaining_length')

    def __init__(self, reader: aio.StreamReader, remaining_length: int) -> None:
        self.reader = reader
        self.remaining_length = remaining_length

    async def read(self, size: ty.Optional[int] = None) -> bytes:
        if size is None:
            size = self.remaining_length

        elif size > self.remaining_length:
            raise ForbiddenReadingError(
                f"You are trying to read {size - self.remaining_length} bytes over the limit")

        self.remaining_length -= size

        return await self.reader.readexactly(size)

    def __bool__(self) -> bool:
        return self.remaining_length > 0


VT = ty.TypeVar('VT')
NVT = ty.TypeVar('NVT')


class TopicMatcher(ty.Generic[VT]):

    @dataclasses.dataclass
    class Node(ty.Generic[NVT]):
        children: ty.Dict[str, 'TopicMatcher.Node[NVT]'] = dataclasses.field(default_factory=dict)
        value: ty.Optional[NVT] = None

    def __init__(self) -> None:
        self._root: 'TopicMatcher.Node[VT]' = self.Node()

    def __setitem__(self, key: str, value: VT) -> None:
        node = self._root
        for topic_token in key.split('/'):
            node = node.children.setdefault(topic_token, self.Node())
        node.value = value

    def __getitem__(self, key: str) -> VT:
        node = self._root
        for topic_token in key.split('/'):
            try:
                node = node.children[topic_token]
            except KeyError as e:
                raise KeyError(key) from e

        if node.value is None:
            raise KeyError(key)

        return node.value

    def get(self, key: str, default: ty.Optional[VT] = None) -> ty.Optional[VT]:
        try:
            return self[key]
        except KeyError:
            return default

    def __delitem__(self, key: str) -> None:
        to_delete: ty.List[ty.Tuple['TopicMatcher.Node[VT]', str, 'TopicMatcher.Node[VT]']] = []
        parent, node = None, self._root
        for topic_token in key.split('/'):
            try:
                parent, node = node, node.children[topic_token]
            except KeyError as e:
                raise KeyError(key) from e

            to_delete.append((parent, topic_token, node))

        for parent, topic_token, node in reversed(to_delete):
            if node.children:
                break
            del parent.children[topic_token]

    def iter_match(self, topic_name: str) -> ty.Generator[VT, None, None]:
        topic_tokens = topic_name.split('/')
        is_system = topic_name.startswith('$')

        def rec(node: 'TopicMatcher.Node[VT]', i: int = 0) -> ty.Generator[VT, None, None]:
            if i == len(topic_tokens):
                if node.value is not None:
                    yield node.value
            else:
                topic_token = topic_tokens[i]
                if topic_token in node.children:
                    yield from rec(node.children[topic_token], i + 1)

                if '+' in node.children and (not is_system or i > 0):
                    yield from rec(node.children['+'], i + 1)

            if '#' in node.children and (not is_system or i > 0):
                value = node.children['#'].value
                if value is not None:
                    yield value

        return rec(self._root)
