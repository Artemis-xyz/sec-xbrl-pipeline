"""
Tests for the SEC pipeline core — the importable surface that Modal uses.

Tests the fetch → parse pipeline and validates output structure matches
what's needed for Snowflake ingestion.

NOTE: XBRL parsing tests require Arelle (run inside Docker):
    docker compose exec app pytest tests/test_sec_pipeline.py -v -s
"""
import json
import pytest
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. SEC API Fetch Tests (no Arelle needed, runs anywhere with network)
# ---------------------------------------------------------------------------

class TestSECFetch:
    """Test SEC EDGAR API client — fetches filing metadata."""

    @pytest.fixture
    def sec_client(self):
        from sec_pipeline import SECAPIClient
        return SECAPIClient(
            user_agent_name="SEC Pipeline Tests",
            user_agent_email="test@example.com",
        )

    async def test_fetch_microsoft_filings(self, sec_client):
        """Fetch MSFT filings and validate the Pydantic response model."""
        from sec_pipeline import XBRLFilingsResponse, XBRLFiling

        response = await sec_client.get_company_filings("MSFT")

        # Validate it's the right Pydantic model
        assert isinstance(response, XBRLFilingsResponse)
        assert response.ticker == "MSFT"
        assert response.cik is not None
        assert response.company_name is not None
        assert response.total_filings > 0

        # Company metadata populated
        assert response.sic_code is not None
        assert response.exchange is not None

        logger.info(f"Fetched {response.total_filings} XBRL filings for {response.company_name}")

    async def test_filings_have_xbrl_urls(self, sec_client):
        """Verify at least some filings have XBRL instance URLs."""
        response = await sec_client.get_company_filings("MSFT")

        filings_with_xbrl = [f for f in response.filings if f.xbrl_instance_url]
        assert len(filings_with_xbrl) > 0, "No filings have XBRL instance URLs"

        # Grab the most recent 10-Q
        ten_qs = [f for f in filings_with_xbrl if f.form_type == "10-Q"]
        assert len(ten_qs) > 0, "No 10-Q filings found"

        latest_10q = ten_qs[0]
        assert latest_10q.is_xbrl is True
        assert latest_10q.xbrl_instance_url.endswith(".xml")
        assert latest_10q.accession_number is not None
        assert latest_10q.filing_date is not None

        logger.info(f"Latest 10-Q: {latest_10q.accession_number} filed {latest_10q.filing_date}")
        logger.info(f"  XBRL URL: {latest_10q.xbrl_instance_url}")

    async def test_fetch_filings_by_cik(self, sec_client):
        """Fetch filings by CIK (skipping ticker lookup) and validate response."""
        from sec_pipeline import XBRLFilingsResponse

        response = await sec_client.get_company_filings_by_cik("0001783879")

        assert isinstance(response, XBRLFilingsResponse)
        assert response.cik == "0001783879"
        assert response.company_name is not None
        assert response.total_filings > 0
        assert response.ticker is not None  # HOOD should resolve

        logger.info(
            f"Fetched {response.total_filings} XBRL filings for "
            f"{response.company_name} ({response.ticker}) via CIK"
        )

    async def test_filing_schema_fields(self, sec_client):
        """Validate all expected fields on XBRLFiling are populated."""
        response = await sec_client.get_company_filings("MSFT")

        filing = response.filings[0]
        assert filing.accession_number is not None
        assert filing.filing_date is not None
        assert filing.form_type is not None
        assert filing.is_xbrl is True

        # Verify the model can serialize cleanly (this is what goes to Snowflake)
        filing_dict = filing.model_dump()
        assert "accession_number" in filing_dict
        assert "filing_date" in filing_dict
        assert "xbrl_instance_url" in filing_dict
        logger.info(f"Filing model_dump keys: {sorted(filing_dict.keys())}")


# ---------------------------------------------------------------------------
# 2. XBRL Parse Tests (requires Arelle — run inside Docker)
# ---------------------------------------------------------------------------

class TestXBRLParse:
    """Test XBRL parsing via Arelle — validates output data structure.

    These tests hit SEC EDGAR and parse real XBRL filings.
    Run inside Docker: docker compose exec app pytest tests/test_sec_pipeline.py::TestXBRLParse -v -s
    """

    @pytest.fixture(scope="class")
    async def parsed_10q(self):
        """Fetch the latest MSFT 10-Q and parse it. Session-scoped to avoid re-parsing."""
        from sec_pipeline import SECAPIClient, XBRLParserService

        client = SECAPIClient(
            user_agent_name="SEC Pipeline Tests",
            user_agent_email="test@example.com",
        )
        parser = XBRLParserService(
            user_agent_name="SEC Pipeline Tests",
            user_agent_email="test@example.com",
        )

        # Fetch filings
        response = await client.get_company_filings("MSFT")
        ten_qs = [f for f in response.filings if f.form_type == "10-Q" and f.xbrl_instance_url]
        assert len(ten_qs) > 0, "No 10-Q filings with XBRL URLs found"

        latest = ten_qs[0]
        logger.info(f"Parsing MSFT 10-Q: {latest.accession_number} ({latest.filing_date})")
        logger.info(f"  URL: {latest.xbrl_instance_url}")

        # Parse XBRL
        xbrl_data = await parser.parse_xbrl_from_url(latest.xbrl_instance_url)

        return {
            "filing": latest,
            "xbrl_data": xbrl_data,
            "company": response,
        }

    async def test_xbrl_output_has_required_keys(self, parsed_10q):
        """The XBRL parser output dict must have all expected top-level keys."""
        xbrl_data = parsed_10q["xbrl_data"]

        required_keys = [
            "document_info",
            "contexts",
            "units",
            "facts",
            "concepts",
            "labels",
            "role_definitions",
            "presentation_relationships",
            "calculation_relationships",
            "definition_relationships",
            "summary",
        ]

        for key in required_keys:
            assert key in xbrl_data, f"Missing required key: {key}"
            logger.info(f"  {key}: {type(xbrl_data[key]).__name__} ({len(xbrl_data[key]) if isinstance(xbrl_data[key], list) else 'dict'})")

    async def test_facts_structure(self, parsed_10q):
        """Each fact should have the fields needed for Snowflake."""
        facts = parsed_10q["xbrl_data"]["facts"]

        assert len(facts) > 100, f"Expected 100+ facts, got {len(facts)}"
        logger.info(f"Total facts: {len(facts)}")

        # Check first fact structure
        fact = facts[0]
        required_fact_fields = ["concept", "concept_name", "context_ref", "value"]
        for field in required_fact_fields:
            assert field in fact, f"Fact missing required field: {field}"

        # Check that numeric facts have proper metadata
        numeric_facts = [f for f in facts if f.get("is_numeric")]
        assert len(numeric_facts) > 50, f"Expected 50+ numeric facts, got {len(numeric_facts)}"

        # Spot check: there should be revenue or net income
        concept_names = {f["concept_name"] for f in facts}
        financial_concepts = {"Revenue", "Revenues", "NetIncomeLoss", "Assets", "CashAndCashEquivalentsAtCarryingValue"}
        found = concept_names & financial_concepts
        assert len(found) > 0, f"Expected at least one of {financial_concepts}, found: {concept_names & financial_concepts}"
        logger.info(f"Found key financial concepts: {found}")

    async def test_facts_have_period_info(self, parsed_10q):
        """Facts should have period information (critical for time-series in Snowflake)."""
        facts = parsed_10q["xbrl_data"]["facts"]

        facts_with_period = [f for f in facts if "period" in f and f["period"]]
        assert len(facts_with_period) > len(facts) * 0.8, "Most facts should have period info"

        # Check period types
        period_types = {f["period"]["type"] for f in facts_with_period if "type" in f["period"]}
        assert "instant" in period_types, "Should have instant periods (balance sheet items)"
        assert "duration" in period_types, "Should have duration periods (income statement items)"
        logger.info(f"Period types found: {period_types}")

    async def test_facts_have_labels(self, parsed_10q):
        """Most facts should have human-readable labels (for Snowflake display)."""
        facts = parsed_10q["xbrl_data"]["facts"]

        facts_with_labels = [f for f in facts if f.get("label")]
        label_pct = len(facts_with_labels) / len(facts) * 100
        assert label_pct > 70, f"Only {label_pct:.0f}% of facts have labels, expected >70%"
        logger.info(f"Facts with labels: {len(facts_with_labels)}/{len(facts)} ({label_pct:.0f}%)")

    async def test_contexts_structure(self, parsed_10q):
        """Contexts define the reporting periods and dimensional breakdowns."""
        contexts = parsed_10q["xbrl_data"]["contexts"]

        assert len(contexts) > 10, f"Expected 10+ contexts, got {len(contexts)}"

        context = contexts[0]
        assert "id" in context
        assert "entity" in context
        assert "period" in context
        logger.info(f"Total contexts: {len(contexts)}")

    async def test_units_structure(self, parsed_10q):
        """Units should include USD, shares, and USD/share."""
        units = parsed_10q["xbrl_data"]["units"]

        assert len(units) > 0, "No units found"

        unit_types = {u.get("unit_type") for u in units}
        logger.info(f"Unit types: {unit_types}")

        # Should have at least simple units (USD, shares)
        assert "simple" in unit_types, "Missing simple units (USD, shares)"

        # Check for common measure names
        all_measures = []
        for u in units:
            if u.get("measure"):
                all_measures.append(u["measure"])
            if u.get("numerator_measure"):
                all_measures.append(u["numerator_measure"])

        measures_str = " ".join(all_measures).lower()
        assert "usd" in measures_str, "USD unit missing"
        logger.info(f"Total units: {len(units)}, measures: {all_measures}")

    async def test_concepts_structure(self, parsed_10q):
        """Concepts are taxonomy definitions — needed for joining across filings."""
        concepts = parsed_10q["xbrl_data"]["concepts"]

        assert len(concepts) > 100, f"Expected 100+ concepts, got {len(concepts)}"

        concept = concepts[0]
        assert "qname" in concept
        assert "local_name" in concept
        assert "is_numeric" in concept
        logger.info(f"Total concepts: {len(concepts)}")

    async def test_role_definitions_capture_all_roles(self, parsed_10q):
        """Role definitions should capture all active presentation roles."""
        roles = parsed_10q["xbrl_data"]["role_definitions"]

        assert len(roles) > 0, "No role definitions found"

        # Validate required fields on each role
        required_fields = {"role_uri", "definition", "category", "description"}
        for role in roles:
            for field in required_fields:
                assert field in role, f"Role missing required field: {field}"

        # Should have multiple categories (Statement, Disclosure, Document, etc.)
        categories = {r["category"] for r in roles if r["category"]}
        logger.info(f"Role categories: {categories}")
        assert len(categories) >= 2, f"Expected at least 2 role categories, got: {categories}"

        # Should include Statement-category roles with recognizable names
        statement_descriptions = {r["description"] for r in roles if r.get("category", "").lower() == "statement"}
        keywords = ["balance sheet", "income", "cash flow", "operations", "equity", "financial position"]
        names_lower = " ".join(statement_descriptions).lower()
        matched = [kw for kw in keywords if kw in names_lower]
        assert len(matched) >= 2, f"Expected at least 2 financial statement keywords, matched: {matched}"

    async def test_presentation_relationships_form_hierarchy(self, parsed_10q):
        """Presentation relationships define the line-item hierarchy."""
        pres = parsed_10q["xbrl_data"]["presentation_relationships"]

        assert len(pres) > 50, f"Expected 50+ presentation relationships, got {len(pres)}"

        rel = pres[0]
        assert "parent_concept" in rel
        assert "child_concept" in rel
        assert "role_uri" in rel
        logger.info(f"Total presentation relationships: {len(pres)}")

    async def test_calculation_relationships_have_weights(self, parsed_10q):
        """Calculation relationships should have weights (+1 or -1)."""
        calcs = parsed_10q["xbrl_data"]["calculation_relationships"]

        assert len(calcs) > 0, "No calculation relationships found"

        weights = {c.get("weight") for c in calcs}
        assert 1.0 in weights, "Missing positive weight (+1)"
        logger.info(f"Total calculation relationships: {len(calcs)}, weights: {weights}")

    async def test_labels_from_linkbase(self, parsed_10q):
        """Labels from the label linkbase provide company-specific names."""
        labels = parsed_10q["xbrl_data"]["labels"]

        assert len(labels) > 50, f"Expected 50+ labels, got {len(labels)}"

        label = labels[0]
        assert "concept_qname" in label
        assert "label_text" in label
        assert "label_role" in label
        logger.info(f"Total labels: {len(labels)}")

    async def test_full_output_is_json_serializable(self, parsed_10q):
        """The entire output must be JSON-serializable (for Snowflake variant column)."""
        import json

        xbrl_data = parsed_10q["xbrl_data"]
        json_str = json.dumps(xbrl_data)
        assert len(json_str) > 1000, "JSON output too small"
        logger.info(f"Total JSON size: {len(json_str):,} bytes")


# ---------------------------------------------------------------------------
# 3. Integration: End-to-End Pipeline (fetch → parse → validate for Snowflake)
# ---------------------------------------------------------------------------

class TestEndToEnd:
    """Full pipeline test: fetch filing list → pick a 10-Q → parse → validate output."""

    async def test_pipeline_produces_snowflake_ready_output(self):
        """Simulate what the Modal pipeline does: fetch, parse, validate for Snowflake."""
        from sec_pipeline import SECAPIClient, XBRLParserService

        # Step 1: Create clients (like Modal worker would)
        client = SECAPIClient(
            user_agent_name="SEC Pipeline E2E Test",
            user_agent_email="test@example.com",
        )
        parser = XBRLParserService(
            user_agent_name="SEC Pipeline E2E Test",
            user_agent_email="test@example.com",
        )

        # Step 2: Fetch filings
        response = await client.get_company_filings("MSFT")
        ten_qs = [f for f in response.filings if f.form_type == "10-Q" and f.xbrl_instance_url]
        latest = ten_qs[0]

        # Step 3: Parse XBRL
        xbrl_data = await parser.parse_xbrl_from_url(latest.xbrl_instance_url)

        # Step 4: Validate Snowflake-ready structure
        # This is what you'd transform into DataFrames and write_to_sf()

        # Facts table
        facts = xbrl_data["facts"]
        assert len(facts) > 0
        for fact in facts[:5]:
            # Every fact should have enough info for a Snowflake row
            assert fact.get("concept") is not None
            assert fact.get("value") is not None or fact.get("is_numeric") is False

        # Filing metadata (from Pydantic model)
        filing_meta = latest.model_dump()
        assert filing_meta["accession_number"]
        assert filing_meta["form_type"] == "10-Q"

        # Company metadata
        assert response.ticker == "MSFT"
        assert response.cik

        logger.info("=" * 60)
        logger.info("End-to-End Pipeline Results:")
        logger.info(f"  Company: {response.company_name} ({response.ticker})")
        logger.info(f"  Filing:  {latest.form_type} {latest.accession_number}")
        logger.info(f"  Facts:   {len(facts)}")
        logger.info(f"  Contexts: {len(xbrl_data['contexts'])}")
        logger.info(f"  Units:   {len(xbrl_data['units'])}")
        logger.info(f"  Concepts: {len(xbrl_data['concepts'])}")
        logger.info(f"  Role Definitions: {len(xbrl_data['role_definitions'])}")
        logger.info(f"  Presentation Rels: {len(xbrl_data['presentation_relationships'])}")
        logger.info(f"  Calculation Rels: {len(xbrl_data['calculation_relationships'])}")
        logger.info(f"  Definition Rels: {len(xbrl_data['definition_relationships'])}")
        logger.info(f"  Labels: {len(xbrl_data['labels'])}")
        logger.info("=" * 60)


# ---------------------------------------------------------------------------
# 4. ParseLogger Unit Tests (no Arelle needed, runs locally)
# ---------------------------------------------------------------------------

class TestParseLogger:
    """Unit tests for the structured parse-time error logger.

    Imports parse_logger directly from its file path to avoid triggering
    the sec_pipeline package init (which requires Arelle).
    """

    @pytest.fixture
    def parse_logger_module(self):
        """Load parse_logger module without triggering the arelle-dependent package chain."""
        import importlib.util
        import pathlib

        module_path = pathlib.Path(__file__).resolve().parent.parent / "sec_pipeline" / "transformation" / "parse_logger.py"
        spec = importlib.util.spec_from_file_location("parse_logger", module_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    @pytest.fixture
    def ParseLogger(self, parse_logger_module):
        """Load ParseLogger without triggering the arelle-dependent package chain."""
        return parse_logger_module.ParseLogger

    @pytest.fixture
    def Severity(self, parse_logger_module):
        """Load Severity enum without triggering the arelle-dependent package chain."""
        return parse_logger_module.Severity

    def test_empty_log_structure(self, ParseLogger):
        """An unused ParseLogger should produce a valid structure with zero counts."""
        log = ParseLogger()
        result = log.to_dict()

        assert result["error_count"] == 0
        assert result["warning_count"] == 0
        assert result["info_count"] == 0
        assert result["entries"] == []
        assert isinstance(result["elapsed_seconds"], float)
        assert result["elapsed_seconds"] >= 0

    def test_error_entries_capture_all_fields(self, ParseLogger):
        """log_error should record all provided keyword arguments."""
        log = ParseLogger()
        exc = AttributeError("'NoneType' has no attribute 'label'")
        log.log_error(
            "facts",
            exc,
            concept="us-gaap:Revenue",
            context_ref="c-123",
            field="label",
            detail="extra info",
        )

        result = log.to_dict()
        assert result["error_count"] == 1

        entry = result["entries"][0]
        assert entry["severity"] == "error"
        assert entry["section"] == "facts"
        assert entry["source_type"] == "AttributeError"
        assert entry["message"] == "'NoneType' has no attribute 'label'"
        assert entry["concept"] == "us-gaap:Revenue"
        assert entry["context_ref"] == "c-123"
        assert entry["field"] == "label"
        assert entry["detail"] == "extra info"

    def test_optional_fields_omitted_when_none(self, ParseLogger):
        """Only provided kwargs should appear in the entry dict."""
        log = ParseLogger()
        log.log_error("concepts", ValueError("bad"), concept="us-gaap:Assets")

        entry = log.to_dict()["entries"][0]
        assert entry["severity"] == "error"
        assert "concept" in entry
        assert "context_ref" not in entry
        assert "field" not in entry
        assert "detail" not in entry

    def test_multiple_errors_accumulate(self, ParseLogger):
        """Multiple log_error calls should all be recorded."""
        log = ParseLogger()
        for i in range(5):
            log.log_error("facts", RuntimeError(f"err {i}"), concept=f"concept-{i}")

        result = log.to_dict()
        assert result["error_count"] == 5
        assert len(result["entries"]) == 5

    def test_output_is_json_serializable(self, ParseLogger):
        """The full to_dict() output must survive json.dumps."""
        log = ParseLogger()
        log.log_error("facts", TypeError("test"), concept="x:Y", field="label")
        log.log_warning("concepts", ValueError("bad value"), concept="z:W")

        serialized = json.dumps(log.to_dict())
        deserialized = json.loads(serialized)
        assert deserialized["error_count"] == 1
        assert deserialized["warning_count"] == 1
        assert len(deserialized["entries"]) == 2

    # ── New severity tests ──

    def test_severity_levels_tracked_separately(self, ParseLogger):
        """Log one of each severity and verify counts are independent."""
        log = ParseLogger()
        log.log_error("facts", RuntimeError("broken"), concept="a:B")
        log.log_warning("concepts", ValueError("degraded"), concept="c:D")
        log.log_info("facts", "cosmetic note", concept="e:F")

        result = log.to_dict()
        assert result["error_count"] == 1
        assert result["warning_count"] == 1
        assert result["info_count"] == 1
        assert len(result["entries"]) == 3

    def test_severity_field_present_on_each_entry(self, ParseLogger, Severity):
        """Every entry must have a valid severity value."""
        log = ParseLogger()
        log.log_error("facts", RuntimeError("e"))
        log.log_warning("facts", RuntimeError("w"))
        log.log_info("facts", "i")

        valid_severities = {s.value for s in Severity}
        for entry in log.to_dict()["entries"]:
            assert "severity" in entry
            assert entry["severity"] in valid_severities

    def test_string_error_produces_observation_type(self, ParseLogger):
        """When a plain string is passed, source_type should be 'Observation'."""
        log = ParseLogger()
        log.log_warning("facts", "Label fallback used", concept="us-gaap:Revenue", field="label")

        entry = log.to_dict()["entries"][0]
        assert entry["source_type"] == "Observation"
        assert entry["message"] == "Label fallback used"
        assert entry["severity"] == "warning"

    def test_exception_preserves_type_name(self, ParseLogger):
        """When an exception is passed, source_type should be the class name."""
        log = ParseLogger()
        log.log_error("concepts", KeyError("missing"), concept="us-gaap:Assets")

        entry = log.to_dict()["entries"][0]
        assert entry["source_type"] == "KeyError"
        assert entry["severity"] == "error"

    def test_log_info_records_correctly(self, ParseLogger):
        """Info severity with field kwarg should be recorded accurately."""
        log = ParseLogger()
        log.log_info("facts", "No iXBRL source", concept="us-gaap:Revenue", field="ixbrl_source")

        result = log.to_dict()
        assert result["info_count"] == 1
        assert result["error_count"] == 0
        assert result["warning_count"] == 0

        entry = result["entries"][0]
        assert entry["severity"] == "info"
        assert entry["section"] == "facts"
        assert entry["field"] == "ixbrl_source"
        assert entry["concept"] == "us-gaap:Revenue"

    # ── Null logger tests ──

    def test_null_logger_is_silent(self, parse_logger_module):
        """NULL_PARSE_LOGGER should accept all calls without raising."""
        null = parse_logger_module.NULL_PARSE_LOGGER
        null.log_error("facts", RuntimeError("boom"), concept="x:Y")
        null.log_warning("facts", "degraded", field="label")
        null.log_info("facts", "cosmetic")
        null.log_aggregate(parse_logger_module.Severity.WARNING, "facts", "5 facts: missing label", count=5)

    def test_null_logger_returns_empty_dict(self, parse_logger_module):
        """NULL_PARSE_LOGGER.to_dict() should return zero counts and no entries."""
        result = parse_logger_module.NULL_PARSE_LOGGER.to_dict()
        assert result["error_count"] == 0
        assert result["warning_count"] == 0
        assert result["info_count"] == 0
        assert result["elapsed_seconds"] == 0.0
        assert result["entries"] == []

    # ── Aggregate tests ──

    def test_log_aggregate_records_summary_entry(self, ParseLogger, Severity):
        """log_aggregate should create a single Aggregate entry with count."""
        log = ParseLogger()
        log.log_aggregate(
            Severity.WARNING, "facts", "42 facts: Numeric fact missing unit_ref",
            count=42, field="unit_ref",
        )

        result = log.to_dict()
        assert result["warning_count"] == 1
        assert len(result["entries"]) == 1

        entry = result["entries"][0]
        assert entry["severity"] == "warning"
        assert entry["section"] == "facts"
        assert entry["source_type"] == "Aggregate"
        assert entry["message"] == "42 facts: Numeric fact missing unit_ref"
        assert entry["count"] == 42
        assert entry["field"] == "unit_ref"

    def test_log_aggregate_without_field(self, ParseLogger, Severity):
        """log_aggregate should omit field key when not provided."""
        log = ParseLogger()
        log.log_aggregate(Severity.INFO, "facts", "10 facts: cosmetic", count=10)

        entry = log.to_dict()["entries"][0]
        assert "field" not in entry
        assert entry["count"] == 10

    def test_log_aggregate_is_json_serializable(self, ParseLogger, Severity):
        """Aggregate entries must survive json.dumps."""
        log = ParseLogger()
        log.log_aggregate(Severity.WARNING, "facts", "5 facts: test", count=5, field="x")

        serialized = json.dumps(log.to_dict())
        deserialized = json.loads(serialized)
        assert deserialized["entries"][0]["source_type"] == "Aggregate"
        assert deserialized["entries"][0]["count"] == 5
