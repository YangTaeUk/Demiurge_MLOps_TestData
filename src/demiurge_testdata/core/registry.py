"""Generic Registry — 데코레이터 기반 자동 등록 팩토리"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.exceptions import RegistryDuplicateError, RegistryKeyError


class Registry:
    """컴포넌트를 문자열 키로 등록하고 인스턴스를 생성하는 범용 레지스트리.

    4개 인스턴스로 전체 프로젝트의 70개+ 구현체를 관리한다:
    - adapter_registry  (22개 어댑터)
    - format_registry   (10종 포맷 핸들러)
    - compression_registry (6종 압축 + none)
    - generator_registry (32종 제너레이터)
    """

    def __init__(self, name: str):
        self._name = name
        self._registry: dict[str, type] = {}

    @property
    def name(self) -> str:
        return self._name

    def register(self, key: str):
        """데코레이터: 클래스를 키와 함께 등록"""

        def decorator(cls: type) -> type:
            if key in self._registry:
                raise RegistryDuplicateError(self._name, key)
            self._registry[key] = cls
            return cls

        return decorator

    def register_class(self, key: str, cls: type) -> None:
        """프로그래밍 방식 등록 (for-loop 일괄 등록용)"""
        if key in self._registry:
            raise RegistryDuplicateError(self._name, key)
        self._registry[key] = cls

    def create(self, key: str, **kwargs: Any) -> Any:
        """키로 인스턴스 생성"""
        if key not in self._registry:
            raise RegistryKeyError(self._name, key, self.list_registered())
        cls = self._registry[key]
        return cls(**kwargs)

    def get_class(self, key: str) -> type:
        """키로 등록된 클래스 반환 (인스턴스 생성 없이)"""
        if key not in self._registry:
            raise RegistryKeyError(self._name, key, self.list_registered())
        return self._registry[key]

    def list_registered(self) -> list[str]:
        """등록된 모든 키 목록"""
        return list(self._registry.keys())

    def __contains__(self, key: str) -> bool:
        return key in self._registry

    def __len__(self) -> int:
        return len(self._registry)

    def __repr__(self) -> str:
        return f"Registry('{self._name}', keys={self.list_registered()})"


# 4개 글로벌 레지스트리 인스턴스
adapter_registry = Registry("adapter")
format_registry = Registry("format")
compression_registry = Registry("compression")
generator_registry = Registry("generator")
