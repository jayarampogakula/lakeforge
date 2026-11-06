"""
LakeForge XML Loader Module
Handles XML file ingestion using Spark's built-in XML data source,
with automatic row-tag detection.
"""
import re
from pathlib import Path
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame


class XMLLoader:
    """
    XML file loader with row-tag auto-detection and attribute handling.

    Uses the `xml` data source available on Databricks (spark-xml).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def detect_row_tag(self, file_path: str, sample_size: int = 65536) -> Optional[str]:
        """
        Detect the repeating record element (row tag) of an XML file
        by sampling the first bytes and counting opening tags.

        Args:
            file_path: Path to the XML file (must be locally readable:
                       /Volumes, /Workspace, or driver-local paths)
            sample_size: Number of bytes to sample

        Returns:
            Most frequently repeated opening tag name, or None if undetectable.
        """
        try:
            p = Path(file_path)
            if p.is_dir():
                # Directories: sample the first data file inside
                data_files = sorted(
                    f for f in p.iterdir()
                    if f.is_file() and not f.name.startswith((".", "_"))
                    and f.suffix.lower() in (".xml", "")
                )
                if not data_files:
                    return None
                p = data_files[0]
            if not p.is_file():
                return None
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                sample = f.read(sample_size)

            tags = re.findall(r"<([A-Za-z_][\w.:-]*)[^>/]*>", sample)
            if not tags:
                return None

            counts: Dict[str, int] = {}
            for tag in tags:
                counts[tag] = counts.get(tag, 0) + 1

            # Most repeated non-root tag wins; skip xml/processing declarations
            candidates = [t for t in counts if not t.startswith(("xml", "?"))]
            if not candidates:
                return None
            return max(candidates, key=lambda t: counts[t])
        except Exception:
            return None

    def load(
        self,
        file_path: str,
        row_tag: Optional[str] = None,
        root_tag: Optional[str] = None,
        exclude_attribute: bool = False,
        attribute_prefix: str = "@",
        value_tag: str = "_VALUE",
        null_value: Optional[str] = None,
        schema: Optional[Any] = None,
        options: Optional[Dict[str, str]] = None,
        auto_detect_row_tag: bool = True,
    ) -> DataFrame:
        """
        Load XML file into a Spark DataFrame.

        Args:
            file_path: Path to XML file or directory (local, /Volumes, dbfs:/, s3a://)
            row_tag: XML element that identifies a record row
                    (auto-detected if None and file is locally readable)
            root_tag: Root element of the document (optional)
            exclude_attribute: Whether to exclude XML attributes from columns
            attribute_prefix: Prefix applied to attribute-derived columns
            value_tag: Tag for the value of an element with attributes
            null_value: Value to treat as null
            schema: Optional explicit schema to apply
            options: Additional spark-xml reader options
            auto_detect_row_tag: Whether to attempt row-tag detection when row_tag is None

        Returns:
            Spark DataFrame
        """
        if row_tag is None and auto_detect_row_tag:
            detected = self.detect_row_tag(file_path)
            if detected:
                row_tag = detected

        reader = self.spark.read.format("xml")

        if row_tag:
            reader = reader.option("rowTag", row_tag)
        if root_tag:
            reader = reader.option("rootTag", root_tag)
        if exclude_attribute:
            reader = reader.option("excludeAttribute", "true")
        if attribute_prefix:
            reader = reader.option("attributePrefix", attribute_prefix)
        if value_tag:
            reader = reader.option("valueTag", value_tag)
        if null_value is not None:
            reader = reader.option("nullValue", null_value)

        reader = reader.option("mode", "PERMISSIVE")
        reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")

        if schema:
            reader = reader.schema(schema)

        if options:
            reader = reader.options(**options)

        return reader.load(file_path)

    def get_sample_data(self, file_path: str, num_rows: int = 100, **load_options) -> DataFrame:
        """
        Load a sample of XML data for preview/validation.
        """
        return self.load(file_path, **load_options).limit(num_rows)


def create_xml_loader(spark: Optional[SparkSession] = None) -> XMLLoader:
    """
    Factory function for XMLLoader.
    """
    if spark is None:
        spark = SparkSession.builder.appName("LakeForge-XML-Loader").getOrCreate()
    return XMLLoader(spark)
