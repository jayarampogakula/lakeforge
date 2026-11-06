"""
LakeForge Universal Loader Module
Single entry point to load ANY supported file format into a Spark DataFrame.

Supported formats:
  - csv, tsv, psv (delimited)          -> Spark CSV data source
  - json, jsonl, ndjson                 -> Spark JSON data source
  - parquet                             -> Spark Parquet data source
  - avro                                -> Spark Avro data source
  - orc                                 -> Spark ORC data source
  - xml                                 -> Spark XML data source (row-tag auto-detect)
  - xlsx, xls, xlsm (Excel)             -> pandas + Spark (multi-sheet support)
  - delta                               -> Delta Lake (with time travel)
  - txt, dat (text / fixed-width)       -> Spark text / pandas read_fwf

The loader auto-detects the format from the file extension when it is not
explicitly provided, and can load entire directories of a single format or
directories containing a mix of formats.
"""
import os
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple

from pyspark.sql import SparkSession, DataFrame

from lakeforge.ingestion.files.xml_loader import XMLLoader
from lakeforge.ingestion.files.avro_loader import AvroLoader
from lakeforge.ingestion.files.orc_loader import ORCLoader
from lakeforge.ingestion.files.delta_loader import DeltaLoader
from lakeforge.ingestion.files.text_loader import TextLoader
from lakeforge.ingestion.files.excel_loader import ExcelLoader


# Map of file extension (lowercase, no dot) -> canonical format name
EXTENSION_TO_FORMAT = {
    "csv": "csv",
    "tsv": "csv",
    "psv": "csv",
    "json": "json",
    "jsonl": "json",
    "ndjson": "json",
    "parquet": "parquet",
    "pq": "parquet",
    "avro": "avro",
    "orc": "orc",
    "xml": "xml",
    "xlsx": "excel",
    "xls": "excel",
    "xlsm": "excel",
    "delta": "delta",
    "txt": "text",
    "dat": "text",
    "log": "text",
}

# Aliases accepted when the format is provided by configuration
FORMAT_ALIASES = {
    "excel": "excel",
    "xlsx": "excel",
    "xls": "excel",
    "xlsm": "excel",
    "tsv": "csv",
    "psv": "csv",
    "jsonl": "json",
    "ndjson": "json",
    "nd": "json",
    "fixed_width": "text",
    "fixedwidth": "text",
    "fwf": "text",
    "text": "text",
    "txt": "text",
    "flat_file": "text",
}

SUPPORTED_FORMATS = sorted(
    {"csv", "json", "parquet", "avro", "orc", "xml", "excel", "delta", "text"}
)


class UniversalLoader:
    """
    Format-agnostic file loader.

    Dispatches to the correct LakeForge loader based on the file extension
    or an explicitly provided format, normalizing all formats to a Spark
    DataFrame with consistent reader options.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._loaders: Dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # Format detection
    # ------------------------------------------------------------------ #

    @staticmethod
    def detect_format(path: str) -> Optional[str]:
        """
        Detect the canonical format of a path from its file extension.

        For directories, sniffs the contained files: a `_delta_log` folder
        maps to Delta; otherwise the most common extension among the files
        wins. Extension-less directories (e.g. Spark-written part files)
        return None - provide `file_format` explicitly in that case.

        Args:
            path: File or directory path (trailing slash/glob is tolerated)

        Returns:
            Canonical format name, or None when it cannot be determined.
        """
        cleaned = path.rstrip("/").rstrip("*")
        ext = os.path.splitext(cleaned)[1].lstrip(".").lower()
        fmt = EXTENSION_TO_FORMAT.get(ext)
        if fmt:
            return fmt

        # Directory detection: sniff contained files
        try:
            p = Path(cleaned)
            if p.is_dir():
                if (p / "_delta_log").is_dir():
                    return "delta"
                counts: Dict[str, int] = {}
                for child in p.iterdir():
                    if child.is_file() and not child.name.startswith((".", "_")):
                        child_fmt = EXTENSION_TO_FORMAT.get(
                            child.suffix.lstrip(".").lower()
                        )
                        if child_fmt:
                            counts[child_fmt] = counts.get(child_fmt, 0) + 1
                if counts:
                    return max(counts, key=lambda f: counts[f])
        except Exception:
            pass
        return None

    @classmethod
    def normalize_format(cls, file_format: str) -> str:
        """
        Normalize a user/config-provided format name to its canonical value.

        Args:
            file_format: Format name or alias (e.g. 'xlsx', 'tsv', 'jsonl')

        Returns:
            Canonical format name

        Raises:
            ValueError: When the format is not supported
        """
        fmt = (file_format or "").strip().lower()
        canonical = FORMAT_ALIASES.get(fmt, fmt)
        if canonical not in {"csv", "json", "parquet", "avro", "orc", "xml", "excel", "delta", "text"}:
            raise ValueError(
                f"Unsupported file format '{file_format}'. "
                f"Supported formats: {SUPPORTED_FORMATS}"
            )
        return canonical

    @staticmethod
    def list_supported_formats() -> List[str]:
        """Return the list of supported canonical formats."""
        return list(SUPPORTED_FORMATS)

    # ------------------------------------------------------------------ #
    # Loader access (lazy)
    # ------------------------------------------------------------------ #

    def _get_loader(self, format_name: str) -> Any:
        if format_name not in self._loaders:
            constructors = {
                "xml": XMLLoader,
                "avro": AvroLoader,
                "orc": ORCLoader,
                "delta": DeltaLoader,
                "text": TextLoader,
                "excel": ExcelLoader,
            }
            self._loaders[format_name] = constructors[format_name](self.spark)
        return self._loaders[format_name]

    # ------------------------------------------------------------------ #
    # Core loading
    # ------------------------------------------------------------------ #

    def load(
        self,
        path: str,
        file_format: Optional[str] = None,
        header: bool = True,
        delimiter: Optional[str] = None,
        encoding: str = "utf-8",
        infer_schema: bool = True,
        multi_line: Optional[bool] = None,
        sheet_name: Any = 0,
        row_tag: Optional[str] = None,
        column_specs: Optional[Dict[str, Tuple[int, int]]] = None,
        version_as_of: Optional[int] = None,
        schema: Optional[Any] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        """
        Load any supported file format into a Spark DataFrame.

        Args:
            path: File path, glob, or directory. When `path` points to a
                  directory and `file_format` is provided, all files in the
                  directory are read with that format.
            file_format: Format name or alias. If None, detected from the
                         file extension (required for directories).
            header: Whether the first row contains headers (csv/text).
            delimiter: Field delimiter (csv). Default ',' (tsv files default '\t').
            encoding: File encoding.
            infer_schema: Whether to infer the schema (csv).
            multi_line: Multi-line records (json/csv). Auto-set for json/jsonl.
            sheet_name: Excel sheet name or index (or list/None for all).
            row_tag: XML record element (auto-detected when None).
            column_specs: Fixed-width column spec {name: (start, end)} (text).
            version_as_of: Delta time-travel version.
            schema: Optional explicit schema to apply.
            options: Additional format-specific Spark reader options.

        Returns:
            Spark DataFrame
        """
        # Resolve the target format
        raw_fmt = (file_format or "").strip().lower()
        if file_format:
            fmt = self.normalize_format(file_format)
        else:
            fmt = self.detect_format(path)
            if fmt is None:
                raise ValueError(
                    f"Could not detect the file format for '{path}'. "
                    f"Provide file_format explicitly (one of {SUPPORTED_FORMATS})."
                )

        opts = dict(options or {})

        if fmt == "csv":
            if delimiter is None:
                ext = os.path.splitext(path.rstrip("/").rstrip("*"))[1].lstrip(".").lower()
                if raw_fmt in ("tsv",) or ext in ("tsv",):
                    delimiter = "\t"
                elif raw_fmt in ("psv",) or ext in ("psv",):
                    delimiter = "|"
                else:
                    delimiter = ","
            reader = self.spark.read.format("csv")
            reader = (
                reader
                .option("header", "true" if header else "false")
                .option("delimiter", delimiter)
                .option("encoding", encoding)
                .option("inferSchema", "true" if infer_schema else "false")
                .option("multiLine", "true" if multi_line else "false")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
            )
            if schema:
                reader = reader.schema(schema)
            if opts:
                reader = reader.options(**opts)
            return reader.load(path)

        if fmt == "json":
            # .jsonl/.ndjson are line-delimited; .json may be multi-line records
            if multi_line is None:
                ext = os.path.splitext(path.rstrip("/").rstrip("*"))[1].lstrip(".").lower()
                multi_line = ext in ("json",)
            reader = self.spark.read.format("json")
            reader = (
                reader
                .option("multiLine", "true" if multi_line else "false")
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
            )
            if schema:
                reader = reader.schema(schema)
            if opts:
                reader = reader.options(**opts)
            return reader.load(path)

        if fmt == "parquet":
            reader = self.spark.read.format("parquet")
            if schema:
                reader = reader.schema(schema)
            if opts:
                reader = reader.options(**opts)
            return reader.load(path)

        if fmt == "avro":
            return self._get_loader("avro").load(path, schema=schema, options=opts or None)

        if fmt == "orc":
            return self._get_loader("orc").load(path, schema=schema, options=opts or None)

        if fmt == "xml":
            return self._get_loader("xml").load(
                path, row_tag=row_tag, schema=schema, options=opts or None
            )

        if fmt == "excel":
            return self._get_loader("excel").load_excel(
                path, sheet_name=sheet_name
            )

        if fmt == "delta":
            return self._get_loader("delta").load(
                path, version_as_of=version_as_of, schema=schema, options=opts or None
            )

        if fmt == "text":
            if column_specs:
                return self._get_loader("text").load_fixed_width(
                    path, column_specs=column_specs, header=header, encoding=encoding
                )
            return self._get_loader("text").load_lines(path, options=opts or None)

        raise ValueError(f"Unsupported format '{fmt}'. Supported: {SUPPORTED_FORMATS}")

    # Convenience alias
    load_any = load

    # ------------------------------------------------------------------ #
    # Config-driven loading
    # ------------------------------------------------------------------ #

    def load_from_config(self, ing_config: Any) -> DataFrame:
        """
        Load a source described by a LakeForge IngestionConfig object.

        Args:
            ing_config: lakeforge.metadata.config_parser.IngestionConfig

        Returns:
            Spark DataFrame
        """
        file_format = (
            ing_config.file_format or ing_config.source_type or ""
        )

        # Extra reader options carried in the config (rowTag, multiLine, ...)
        extra_options = dict(getattr(ing_config, "reader_options", None) or {})

        # Pop well-known positional/typed kwargs so they are not double-passed
        options = dict(extra_options)
        row_tag = options.pop("rowTag", None) or options.pop("row_tag", None)
        multi_line = options.pop("multiLine", None)
        if multi_line is not None:
            multi_line = str(multi_line).lower() == "true"
        version_as_of = options.pop("versionAsOf", None)
        if version_as_of is not None:
            version_as_of = int(version_as_of)

        sheet_name = getattr(ing_config, "sheet_name", None)
        if sheet_name is None:
            sheet_name = 0

        return self.load(
            ing_config.source_path,
            file_format=file_format,
            header=ing_config.header,
            delimiter=ing_config.delimiter,
            encoding=ing_config.encoding or "utf-8",
            infer_schema=ing_config.infer_schema,
            multi_line=multi_line,
            sheet_name=sheet_name,
            row_tag=row_tag,
            version_as_of=version_as_of,
            options=options,
        )

    # ------------------------------------------------------------------ #
    # Directory utilities
    # ------------------------------------------------------------------ #

    def load_mixed_directory(
        self, directory: str, **load_options
    ) -> Dict[str, DataFrame]:
        """
        Load a directory containing multiple file formats.

        Files are grouped by extension and one DataFrame is returned per
        detected format.

        Args:
            directory: Directory path (must be locally listable: /Volumes,
                      /Workspace, or driver-local)

        Returns:
            Mapping of format name -> Spark DataFrame
        """
        dir_path = Path(directory)
        if not dir_path.is_dir():
            raise FileNotFoundError(f"Directory not found or not locally listable: {directory}")

        by_format: Dict[str, List[str]] = {}
        for f in sorted(dir_path.rglob("*")):
            if f.is_file() and not f.name.startswith((".", "_")):
                fmt = self.detect_format(str(f))
                if fmt:
                    by_format.setdefault(fmt, []).append(str(f))

        result: Dict[str, DataFrame] = {}
        for fmt, files in by_format.items():
            if fmt == "excel":
                # Excel files must be read individually (pandas), then unioned
                dfs = [self.load(f, file_format=fmt, **load_options) for f in files]
                base = dfs[0]
                for extra in dfs[1:]:
                    base = base.unionByName(extra, allowMissingColumns=True)
                result[fmt] = base
            else:
                # Spark sources accept a directory or comma-joined paths
                result[fmt] = self.load(
                    ",".join(files) if len(files) > 1 else files[0],
                    file_format=fmt,
                    **load_options,
                )
        return result


def create_universal_loader(spark: Optional[SparkSession] = None) -> UniversalLoader:
    """
    Factory function for UniversalLoader.
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-Universal-Loader").getOrCreate()
    return UniversalLoader(spark)
