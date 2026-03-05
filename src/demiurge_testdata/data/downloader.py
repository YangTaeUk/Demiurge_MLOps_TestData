"""Kaggle 데이터셋 다운로드 및 검증"""

from __future__ import annotations

import csv
import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from demiurge_testdata.core.exceptions import DataDownloadError

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """다운로드 검증 결과"""

    dataset: str
    ok: bool
    missing_files: list[str] = field(default_factory=list)
    row_count: int | None = None
    expected_count: int | None = None
    errors: list[str] = field(default_factory=list)


class KaggleDownloader:
    """Kaggle CLI를 통한 데이터셋 다운로드 관리자"""

    def __init__(self, data_dir: Path = Path("data/raw")):
        self._data_dir = data_dir

    def download(self, kaggle_id: str, dataset_name: str) -> Path:
        """Kaggle 일반 데이터셋 다운로드."""
        dest = self._data_dir / dataset_name
        dest.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading dataset %s → %s", kaggle_id, dest)
        result = subprocess.run(
            [
                "kaggle", "datasets", "download",
                "-d", kaggle_id,
                "-p", str(dest),
                "--unzip",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise DataDownloadError(
                f"Kaggle download failed for {kaggle_id}: {result.stderr}"
            )
        logger.info("Downloaded %s successfully", kaggle_id)
        return dest

    def download_competition(self, competition: str, dataset_name: str) -> Path:
        """Kaggle Competition 데이터셋 다운로드."""
        dest = self._data_dir / dataset_name
        dest.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading competition %s → %s", competition, dest)
        result = subprocess.run(
            [
                "kaggle", "competitions", "download",
                "-c", competition,
                "-p", str(dest),
                "--force",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise DataDownloadError(
                f"Kaggle competition download failed for {competition}: {result.stderr}"
            )
        # competition은 zip으로 내려오므로 수동 해제 필요할 수 있음
        self._try_unzip(dest)
        logger.info("Downloaded competition %s successfully", competition)
        return dest

    def download_from_manifest(
        self,
        entry: dict[str, Any],
        dataset_name: str,
    ) -> Path:
        """매니페스트 엔트리 기반 다운로드 (type에 따라 분기)."""
        kaggle_id = entry["kaggle_id"]
        kaggle_type = entry.get("kaggle_type", "dataset")

        if kaggle_type == "competition":
            return self.download_competition(kaggle_id, dataset_name)
        return self.download(kaggle_id, dataset_name)

    def verify(self, dataset_name: str, expected_files: list[str]) -> bool:
        """다운로드된 파일 존재 여부 검증"""
        dest = self._data_dir / dataset_name
        for filename in expected_files:
            if not (dest / filename).exists():
                return False
        return True

    def _try_unzip(self, dest: Path) -> None:
        """dest 내의 zip 파일을 해제한다."""
        for zip_file in dest.glob("*.zip"):
            import zipfile

            with zipfile.ZipFile(zip_file) as zf:
                zf.extractall(dest)
            zip_file.unlink()
            logger.info("Unzipped %s", zip_file.name)


class DataValidator:
    """다운로드된 CSV 파일의 기본 무결성 검증"""

    def __init__(self, data_dir: Path = Path("data/raw")):
        self._data_dir = data_dir

    def validate(
        self,
        dataset_name: str,
        manifest_entry: dict[str, Any],
    ) -> ValidationResult:
        """검증: 파일 존재, CSV 파싱, 행 수 검증."""
        dest = self._data_dir / dataset_name
        result = ValidationResult(dataset=dataset_name, ok=True)

        # 1. 파일 존재 확인
        expected_files = manifest_entry.get("files", [])
        for fname in expected_files:
            if not (dest / fname).exists():
                result.missing_files.append(fname)
                result.ok = False

        if result.missing_files:
            result.errors.append(f"Missing files: {result.missing_files}")
            return result

        # 2. primary_file CSV 파싱 + 행 수 확인
        primary = manifest_entry.get("primary_file")
        if primary and (dest / primary).exists():
            primary_path = dest / primary
            try:
                row_count = self._count_rows(primary_path)
                result.row_count = row_count
                result.expected_count = manifest_entry.get("record_count")

                if result.expected_count:
                    tolerance = result.expected_count * 0.1
                    if abs(row_count - result.expected_count) > tolerance:
                        result.errors.append(
                            f"Row count mismatch: got {row_count}, "
                            f"expected ~{result.expected_count} (±10%)"
                        )
                        result.ok = False
            except Exception as exc:
                result.errors.append(f"CSV parsing error: {exc}")
                result.ok = False

        return result

    def _count_rows(self, path: Path) -> int:
        """CSV 파일의 행 수를 센다 (헤더 제외)."""
        count = 0
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                for _ in reader:
                    count += 1
        except UnicodeDecodeError:
            with open(path, newline="", encoding="latin-1") as f:
                reader = csv.reader(f)
                next(reader, None)
                for _ in reader:
                    count += 1
        return count
