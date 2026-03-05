"""AdapterType, FormatType 등 프로젝트 공통 Enum"""

from enum import StrEnum, unique


@unique
class AdapterCategory(StrEnum):
    RDBMS = "rdbms"
    NOSQL = "nosql"
    STREAMING = "streaming"
    STORAGE = "storage"
    FILE_TRANSFER = "filetransfer"


@unique
class AdapterType(StrEnum):
    # RDBMS
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MARIADB = "mariadb"
    MSSQL = "mssql"
    ORACLE = "oracle"
    SQLITE = "sqlite"
    COCKROACHDB = "cockroachdb"
    BIGQUERY = "bigquery"
    # NoSQL
    MONGODB = "mongodb"
    ELASTICSEARCH = "elasticsearch"
    REDIS = "redis"
    CASSANDRA = "cassandra"
    # Streaming
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"
    MQTT = "mqtt"
    PULSAR = "pulsar"
    NATS = "nats"
    # Storage
    S3 = "s3"
    LOCAL_FS = "local_fs"
    HDFS = "hdfs"
    # FileTransfer
    FTP = "ftp"
    SFTP = "sftp"


@unique
class FormatType(StrEnum):
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    MSGPACK = "msgpack"
    ARROW = "arrow"
    XML = "xml"
    YAML = "yaml"


@unique
class CompressionType(StrEnum):
    NONE = "none"
    GZIP = "gzip"
    BROTLI = "brotli"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"
    LZMA = "lzma"


@unique
class GenerationMode(StrEnum):
    STREAM = "stream"
    BATCH = "batch"
    API = "api"


@unique
class DatasetCategory(StrEnum):
    RELATIONAL = "relational"
    DOCUMENT = "document"
    EVENT = "event"
    IOT = "iot"
    TEXT = "text"
    GEOSPATIAL = "geospatial"
