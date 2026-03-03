"""SEC EDGAR XBRL financial filing ingestion pipeline."""

from sec_pipeline.ingestion.sec_api import SECAPIClient, get_sec_client
from sec_pipeline.ingestion.schemas import XBRLFiling, XBRLFilingsResponse
from sec_pipeline.transformation.xbrl_parser import XBRLParserService
from sec_pipeline.ingestion.sec_url_builder import (
    build_sec_viewer_url,
    build_sec_document_url,
    build_sec_filing_index_url,
    build_fact_sec_urls,
)

__all__ = [
    "SECAPIClient",
    "get_sec_client",
    "XBRLParserService",
    "XBRLFiling",
    "XBRLFilingsResponse",
    "build_sec_viewer_url",
    "build_sec_document_url",
    "build_sec_filing_index_url",
    "build_fact_sec_urls",
]
