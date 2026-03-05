"""DataPipeline 오케스트레이터 — Generator → HandlerChain → Adapter 흐름을 조율한다."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from demiurge_testdata.core.exceptions import PipelineError

if TYPE_CHECKING:
    from demiurge_testdata.adapters.base import BaseAdapter
    from demiurge_testdata.generators.base import BaseGenerator
    from demiurge_testdata.handlers.chain import HandlerChain


@dataclass
class PipelineMetrics:
    """파이프라인 실행 결과 메트릭"""

    total_records: int = 0
    total_bytes: int = 0
    error_count: int = 0
    elapsed_seconds: float = 0.0
    generation_time_ms: float = 0.0
    encoding_time_ms: float = 0.0
    push_time_ms: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def records_per_second(self) -> float:
        if self.elapsed_seconds == 0:
            return 0.0
        return self.total_records / self.elapsed_seconds

    @property
    def compression_ratio(self) -> float:
        return 0.0  # Phase 2에서 raw_bytes 대비 계산


class DataPipeline:
    """Generator → HandlerChain → Adapter 파이프라인 오케스트레이터.

    Pipeline은 구체 클래스가 아닌 추상에 의존한다 (DIP).
    어떤 Generator·Handler·Adapter가 사용되는지 모른다.
    """

    def __init__(
        self,
        generator: BaseGenerator,
        handler_chain: HandlerChain,
        adapter: BaseAdapter,
        batch_size: int = 1000,
    ):
        self._generator = generator
        self._chain = handler_chain
        self._adapter = adapter
        self._batch_size = batch_size

    async def run_batch(self) -> PipelineMetrics:
        """Batch 모드: Generator에서 배치 단위로 생성 → 인코딩 → 적재"""
        metrics = PipelineMetrics()
        start = time.monotonic()

        try:
            # Generate
            t0 = time.monotonic()
            records = await self._generator.batch(self._batch_size)
            metrics.generation_time_ms = (time.monotonic() - t0) * 1000

            # Encode
            t0 = time.monotonic()
            encoded = await self._chain.encode(records)
            metrics.encoding_time_ms = (time.monotonic() - t0) * 1000

            # Push
            t0 = time.monotonic()
            metadata = {
                "format": self._chain.format_name,
                "compression": self._chain.compression_name,
                "record_count": len(records),
            }
            await self._adapter.push(encoded, metadata)
            metrics.push_time_ms = (time.monotonic() - t0) * 1000

            metrics.total_records = len(records)
            metrics.total_bytes = len(encoded)

        except Exception as e:
            metrics.error_count += 1
            metrics.errors.append(str(e))
            raise PipelineError(f"Batch pipeline failed: {e}") from e

        finally:
            metrics.elapsed_seconds = time.monotonic() - start

        return metrics

    async def run_stream(self) -> PipelineMetrics:
        """Stream 모드: Generator에서 레코드 단위로 스트리밍 → 인코딩 → 적재"""
        metrics = PipelineMetrics()
        start = time.monotonic()

        try:
            async for record in self._generator.stream():
                encoded = await self._chain.encode([record])
                metadata = {
                    "format": self._chain.format_name,
                    "compression": self._chain.compression_name,
                    "record_count": 1,
                }
                await self._adapter.push(encoded, metadata)
                metrics.total_records += 1
                metrics.total_bytes += len(encoded)

        except Exception as e:
            metrics.error_count += 1
            metrics.errors.append(str(e))
            raise PipelineError(f"Stream pipeline failed: {e}") from e

        finally:
            metrics.elapsed_seconds = time.monotonic() - start

        return metrics
