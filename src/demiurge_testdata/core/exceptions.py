"""프로젝트 공통 예외 계층"""


class TestDataError(Exception):
    """모든 프로젝트 예외의 기본 클래스"""


class RegistryError(TestDataError):
    """Registry 관련 에러 (중복 키, 미등록 키 등)"""


class RegistryKeyError(RegistryError):
    """등록되지 않은 키로 생성 시도"""

    def __init__(self, registry_name: str, key: str, available: list[str]):
        self.registry_name = registry_name
        self.key = key
        self.available = available
        super().__init__(
            f"Registry '{registry_name}': key '{key}' not found. Available: {available}"
        )


class RegistryDuplicateError(RegistryError):
    """이미 등록된 키로 재등록 시도"""

    def __init__(self, registry_name: str, key: str):
        self.registry_name = registry_name
        self.key = key
        super().__init__(f"Registry '{registry_name}': key '{key}' is already registered")


class ConfigError(TestDataError):
    """설정 로딩/검증 에러"""


class PipelineError(TestDataError):
    """파이프라인 실행 에러"""


class HandlerError(TestDataError):
    """핸들러 인코딩/디코딩 에러"""


class AdapterError(TestDataError):
    """어댑터 연결/전송 에러"""


class GeneratorError(TestDataError):
    """제너레이터 데이터 생성 에러"""


# ── 데이터 수집·시딩 예외 ──


class DataDownloadError(TestDataError):
    """Kaggle API 다운로드 실패"""


class SchemaInferenceError(TestDataError):
    """CSV 컬럼 타입 추론 실패"""


class BulkInsertError(AdapterError):
    """대량 삽입 실패 (재시도 가능)"""

    def __init__(self, adapter: str, table: str, batch_index: int, cause: Exception):
        self.adapter = adapter
        self.table = table
        self.batch_index = batch_index
        self.cause = cause
        super().__init__(
            f"{adapter}: bulk insert failed on {table} "
            f"(batch #{batch_index}): {cause}"
        )
