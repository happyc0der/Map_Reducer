from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class MapRequest(_message.Message):
    __slots__ = ("data", "centroids")
    DATA_FIELD_NUMBER: _ClassVar[int]
    CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedScalarFieldContainer[str]
    centroids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, data: _Optional[_Iterable[str]] = ..., centroids: _Optional[_Iterable[str]] = ...) -> None: ...

class MapResponse(_message.Message):
    __slots__ = ("results",)
    class ResultsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.ScalarMap[str, str]
    def __init__(self, results: _Optional[_Mapping[str, str]] = ...) -> None: ...

class StartReduceRequest(_message.Message):
    __slots__ = ("Flag",)
    FLAG_FIELD_NUMBER: _ClassVar[int]
    Flag: bool
    def __init__(self, Flag: bool = ...) -> None: ...

class StartReduceResponse(_message.Message):
    __slots__ = ("ok",)
    OK_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    def __init__(self, ok: bool = ...) -> None: ...

class ReduceRequest(_message.Message):
    __slots__ = ("key", "values")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    key: str
    values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, key: _Optional[str] = ..., values: _Optional[_Iterable[str]] = ...) -> None: ...

class ReduceResponse(_message.Message):
    __slots__ = ("key", "newCentroid")
    KEY_FIELD_NUMBER: _ClassVar[int]
    NEWCENTROID_FIELD_NUMBER: _ClassVar[int]
    key: str
    newCentroid: str
    def __init__(self, key: _Optional[str] = ..., newCentroid: _Optional[str] = ...) -> None: ...
