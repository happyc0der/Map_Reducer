from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MapRequest(_message.Message):
    __slots__ = ("begin", "end", "centroids", "num_reducers", "append")
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    NUM_REDUCERS_FIELD_NUMBER: _ClassVar[int]
    APPEND_FIELD_NUMBER: _ClassVar[int]
    begin: int
    end: int
    centroids: _containers.RepeatedCompositeFieldContainer[point]
    num_reducers: int
    append: bool
    def __init__(self, begin: _Optional[int] = ..., end: _Optional[int] = ..., centroids: _Optional[_Iterable[_Union[point, _Mapping]]] = ..., num_reducers: _Optional[int] = ..., append: bool = ...) -> None: ...

class MapResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...

class StartReduceRequest(_message.Message):
    __slots__ = ("partitions", "num_mappers")
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    NUM_MAPPERS_FIELD_NUMBER: _ClassVar[int]
    partitions: _containers.RepeatedScalarFieldContainer[int]
    num_mappers: int
    def __init__(self, partitions: _Optional[_Iterable[int]] = ..., num_mappers: _Optional[int] = ...) -> None: ...

class StartReduceResponse(_message.Message):
    __slots__ = ("ok",)
    OK_FIELD_NUMBER: _ClassVar[int]
    ok: int
    def __init__(self, ok: _Optional[int] = ...) -> None: ...

class ReduceRequest(_message.Message):
    __slots__ = ("partitions",)
    PARTITIONS_FIELD_NUMBER: _ClassVar[int]
    partitions: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, partitions: _Optional[_Iterable[int]] = ...) -> None: ...

class returnReduce(_message.Message):
    __slots__ = ("ok",)
    OK_FIELD_NUMBER: _ClassVar[int]
    ok: int
    def __init__(self, ok: _Optional[int] = ...) -> None: ...

class returnReduceResponse(_message.Message):
    __slots__ = ("ok",)
    OK_FIELD_NUMBER: _ClassVar[int]
    ok: int
    def __init__(self, ok: _Optional[int] = ...) -> None: ...

class point(_message.Message):
    __slots__ = ("x", "y")
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    x: float
    y: float
    def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ...) -> None: ...

class centroid_values(_message.Message):
    __slots__ = ("key", "values")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    key: int
    values: _containers.RepeatedCompositeFieldContainer[point]
    def __init__(self, key: _Optional[int] = ..., values: _Optional[_Iterable[_Union[point, _Mapping]]] = ...) -> None: ...

class ReduceResponse(_message.Message):
    __slots__ = ("dictionary",)
    DICTIONARY_FIELD_NUMBER: _ClassVar[int]
    dictionary: _containers.RepeatedCompositeFieldContainer[centroid_values]
    def __init__(self, dictionary: _Optional[_Iterable[_Union[centroid_values, _Mapping]]] = ...) -> None: ...
