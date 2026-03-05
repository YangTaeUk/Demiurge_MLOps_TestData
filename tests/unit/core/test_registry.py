"""Registry 단위 테스트 — 등록/생성/조회/에러"""

import pytest

from demiurge_testdata.core.exceptions import RegistryDuplicateError, RegistryKeyError
from demiurge_testdata.core.registry import Registry


class TestRegistry:
    def setup_method(self):
        self.registry = Registry("test")

    def test_register_and_create(self):
        @self.registry.register("foo")
        class Foo:
            def __init__(self, value=42):
                self.value = value

        instance = self.registry.create("foo")
        assert isinstance(instance, Foo)
        assert instance.value == 42

    def test_register_with_kwargs(self):
        @self.registry.register("bar")
        class Bar:
            def __init__(self, x: int, y: str = "default"):
                self.x = x
                self.y = y

        instance = self.registry.create("bar", x=10, y="custom")
        assert instance.x == 10
        assert instance.y == "custom"

    def test_list_registered(self):
        @self.registry.register("a")
        class A:
            pass

        @self.registry.register("b")
        class B:
            pass

        keys = self.registry.list_registered()
        assert "a" in keys
        assert "b" in keys
        assert len(keys) == 2

    def test_contains(self):
        @self.registry.register("exists")
        class Exists:
            pass

        assert "exists" in self.registry
        assert "missing" not in self.registry

    def test_len(self):
        assert len(self.registry) == 0

        @self.registry.register("one")
        class One:
            pass

        assert len(self.registry) == 1

    def test_duplicate_key_raises(self):
        @self.registry.register("dup")
        class First:
            pass

        with pytest.raises(RegistryDuplicateError, match="already registered"):

            @self.registry.register("dup")
            class Second:
                pass

    def test_missing_key_raises(self):
        with pytest.raises(RegistryKeyError, match="not found"):
            self.registry.create("nonexistent")

    def test_missing_key_shows_available(self):
        @self.registry.register("real")
        class Real:
            pass

        with pytest.raises(RegistryKeyError) as exc_info:
            self.registry.create("fake")

        assert "real" in str(exc_info.value)

    def test_get_class(self):
        @self.registry.register("cls")
        class MyClass:
            pass

        retrieved = self.registry.get_class("cls")
        assert retrieved is MyClass

    def test_get_class_missing_raises(self):
        with pytest.raises(RegistryKeyError):
            self.registry.get_class("nope")

    def test_register_class_programmatic(self):
        class Programmatic:
            def __init__(self, algo="lz4"):
                self.algo = algo

        self.registry.register_class("prog", Programmatic)
        instance = self.registry.create("prog", algo="zstd")
        assert instance.algo == "zstd"

    def test_register_class_duplicate_raises(self):
        class A:
            pass

        self.registry.register_class("x", A)
        with pytest.raises(RegistryDuplicateError):
            self.registry.register_class("x", A)

    def test_repr(self):
        assert "test" in repr(self.registry)

    def test_name_property(self):
        assert self.registry.name == "test"
