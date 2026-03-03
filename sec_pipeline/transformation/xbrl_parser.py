import asyncio
import logging
import re
import html
from typing import Any, Dict, List
from arelle import Cntlr, XbrlConst
from arelle.ModelXbrl import ModelXbrl

from sec_pipeline.transformation.parse_logger import ParseLogger, Severity, NULL_PARSE_LOGGER

logger = logging.getLogger(__name__)


def strip_html(text: str) -> str:
    """
    Strip HTML tags from text and decode HTML entities.

    Args:
        text: Text that may contain HTML tags

    Returns:
        Clean text with HTML tags removed and entities decoded
    """
    if not isinstance(text, str):
        return text

    # Decode HTML entities first (e.g., &nbsp;, &lt;, &gt;)
    text = html.unescape(text)

    # Remove HTML tags using regex
    # This handles most common cases without requiring BeautifulSoup dependency
    text = re.sub(r'<[^>]+>', '', text)

    # Clean up extra whitespace that may result from tag removal
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()

    return text


class XBRLParserService:
    """Service for parsing XBRL documents using Arelle - the SEC's standard XBRL processor."""

    def __init__(
        self,
        user_agent_name: str | None = None,
        user_agent_email: str | None = None,
    ):
        # Initialize Arelle controller (headless, no GUI)
        self.controller = Cntlr.Cntlr(logFileName="logToStdErr")
        self.controller.webCache.timeout = 60

        # Accept explicit values; fall back to settings (which reads env vars / .env)
        if user_agent_name is None or user_agent_email is None:
            from sec_pipeline.core.config import settings
            user_agent_name = user_agent_name or settings.SEC_USER_AGENT_NAME
            user_agent_email = user_agent_email or settings.SEC_USER_AGENT_EMAIL

        # Configure User-Agent for SEC compliance
        user_agent = f"{user_agent_name} {user_agent_email}"
        self.controller.webCache.userAgentHeader = user_agent
        logger.info(f"Initialized Arelle XBRL parser with User-Agent: {user_agent}")

    async def parse_xbrl_from_url(self, url: str) -> Dict[str, Any]:
        """
        Parse an XBRL instance document from a URL using Arelle.

        Args:
            url: URL to the XBRL instance document (.xml or _htm.xml for inline)

        Returns:
            Dict containing all extracted XBRL data in JSON format
        """
        return await asyncio.to_thread(self._sync_parse, url)

    def _sync_parse(self, url: str) -> Dict[str, Any]:
        """Synchronous parse body — runs in a thread pool via to_thread."""
        model_xbrl = None
        try:
            logger.info(f"Loading XBRL document from: {url}")

            # Load the XBRL instance document
            # Arelle will automatically download and process all referenced schemas
            model_xbrl = self.controller.modelManager.load(url)

            if model_xbrl is None:
                raise ValueError("Failed to load XBRL document")

            if model_xbrl.modelDocument is None:
                raise ValueError("XBRL document has no model document")

            logger.info(f"Successfully loaded XBRL document: {model_xbrl.modelDocument.basename}")
            logger.info(f"Found {len(model_xbrl.facts)} facts, {len(model_xbrl.contexts)} contexts, {len(model_xbrl.units)} units")

            # Extract all data into a structured format
            result = self._extract_all_data(model_xbrl)

            return result

        except Exception as e:
            logger.error(f"Error parsing XBRL document: {type(e).__name__}: {str(e)}")
            raise
        finally:
            # Clean up - close the model
            if model_xbrl is not None:
                model_xbrl.close()

    def _extract_all_data(self, model_xbrl: ModelXbrl) -> Dict[str, Any]:
        """
        Extract all data from an Arelle ModelXbrl instance into a JSON-serializable format.

        Args:
            model_xbrl: Loaded Arelle ModelXbrl instance

        Returns:
            Dict containing all facts, contexts, units, taxonomy metadata, and relationships.
            Individual sections that fail are returned as empty lists/dicts with
            errors recorded in parse_log so downstream consumers get partial results.
        """
        parse_log = ParseLogger()

        sections: Dict[str, Any] = {
            "document_info": (self._extract_document_info, {}),
            "contexts": (self._extract_contexts, []),
            "units": (self._extract_units, []),
            "facts": (self._extract_facts, []),
            "concepts": (self._extract_concepts, []),
            "labels": (self._extract_labels, []),
            "role_definitions": (self._extract_role_definitions, []),
            "presentation_relationships": (self._extract_presentation_relationships, []),
            "calculation_relationships": (self._extract_calculation_relationships, []),
            "definition_relationships": (self._extract_definition_relationships, []),
            "summary": (self._generate_summary, {}),
        }

        result: Dict[str, Any] = {}

        for section_name, (extractor, fallback) in sections.items():
            try:
                result[section_name] = extractor(model_xbrl, parse_log=parse_log)
            except Exception as e:
                logger.error(f"Section {section_name!r} failed: {type(e).__name__}: {e}")
                parse_log.log_error(section_name, e, detail=f"Entire {section_name} section failed")
                result[section_name] = fallback

        result["parse_log"] = parse_log.to_dict()

        return result

    def _extract_document_info(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> Dict[str, Any]:
        """Extract document-level metadata."""
        doc_info = {
            "document_type": model_xbrl.modelDocument.type if model_xbrl.modelDocument else None,
            "entity": None
        }

        # Get entity identifier from the first context if available
        try:
            if model_xbrl.contexts:
                first_context = next(iter(model_xbrl.contexts.values()))
                if hasattr(first_context, 'entityIdentifier'):
                    doc_info["entity"] = {
                        "identifier": first_context.entityIdentifier[1],  # [0] is scheme, [1] is identifier
                        "scheme": first_context.entityIdentifier[0]
                    }
        except Exception as e:
            parse_log.log_error("document_info", e, field="entity")

        return doc_info

    def _extract_contexts(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """Extract all contexts from the XBRL instance."""
        contexts = []

        for context_id, context in model_xbrl.contexts.items():
            try:
                context_data = {
                    "id": context_id,
                    "entity": {
                        "identifier": context.entityIdentifier[1],
                        "scheme": context.entityIdentifier[0]
                    },
                    "period": {}
                }

                # Extract period information
                if context.isInstantPeriod:
                    context_data["period"]["type"] = "instant"
                    context_data["period"]["instant"] = str(context.instantDatetime) if context.instantDatetime else None
                elif context.isStartEndPeriod:
                    context_data["period"]["type"] = "duration"
                    context_data["period"]["start_date"] = str(context.startDatetime) if context.startDatetime else None
                    context_data["period"]["end_date"] = str(context.endDatetime) if context.endDatetime else None
                elif context.isForeverPeriod:
                    context_data["period"]["type"] = "forever"

                # Extract dimensions (explicit and typed members)
                dimensions = []
                if hasattr(context, 'qnameDims') and context.qnameDims:
                    for dim_qname, dim_value in context.qnameDims.items():
                        dim_data = {
                            "dimension": str(dim_qname),
                            "type": "explicit" if hasattr(dim_value, 'memberQname') else "typed"
                        }
                        if hasattr(dim_value, 'memberQname'):
                            dim_data["value"] = str(dim_value.memberQname)
                        elif hasattr(dim_value, 'typedMember'):
                            dim_data["value"] = str(dim_value.typedMember.stringValue) if hasattr(dim_value.typedMember, 'stringValue') else str(dim_value.typedMember)
                        dimensions.append(dim_data)

                if dimensions:
                    context_data["dimensions"] = dimensions

                contexts.append(context_data)

            except Exception as e:
                logger.debug(f"Error extracting context {context_id}: {e}")
                parse_log.log_error("contexts", e, context_ref=context_id)
                continue

        parse_log.record_section_count("contexts", len(contexts))
        return contexts

    def _extract_units(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """Extract all units from the XBRL instance."""
        units = []

        for unit_id, unit in model_xbrl.units.items():
            try:
                unit_data = {
                    "id": unit_id,
                    "measures": []
                }

                # Handle simple and divide units
                # Arelle always returns a 2-tuple: (numerator_measures, denominator_measures)
                # Simple units have an empty denominator tuple, e.g. ((iso4217:USD,), ())
                # Divide units have both populated, e.g. ((iso4217:USD,), (shares,))
                if hasattr(unit, 'measures'):
                    if len(unit.measures) == 2 and unit.measures[1]:
                        # Divide unit (e.g., USD/share) — denominator is non-empty
                        unit_data["numerator"] = [str(m) for m in unit.measures[0]]
                        unit_data["denominator"] = [str(m) for m in unit.measures[1]]
                        unit_data["unit_type"] = "divide"
                        unit_data["numerator_measure"] = str(unit.measures[0][0]) if unit.measures[0] else None
                        unit_data["denominator_measure"] = str(unit.measures[1][0]) if unit.measures[1] else None
                        del unit_data["measures"]
                    else:
                        # Simple unit — either 1-tuple or 2-tuple with empty denominator
                        numerator = unit.measures[0] if unit.measures else ()
                        unit_data["measures"] = [str(m) for m in numerator]
                        unit_data["unit_type"] = "simple"
                        unit_data["measure"] = str(numerator[0]) if numerator else None

                units.append(unit_data)

            except Exception as e:
                logger.debug(f"Error extracting unit {unit_id}: {e}")
                parse_log.log_error("units", e, detail=unit_id)
                continue

        parse_log.record_section_count("units", len(units))
        return units

    def _extract_facts(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """Extract all facts from the XBRL instance."""
        facts = []
        validation_counts: dict[tuple, int] = {}

        for fact in model_xbrl.facts:
            raw_value = fact.value
            value = raw_value
            if isinstance(value, str):
                value = strip_html(value)

            fact_data = {
                "concept": str(fact.qname),
                "concept_name": fact.qname.localName,
                "context_ref": fact.contextID,
                "value": value,
                "unit_ref": fact.unitID if hasattr(fact, 'unitID') and fact.unitID else None,
                "is_nil": fact.isNil if hasattr(fact, 'isNil') else False,
            }

            # Preserve raw HTML for text block values (tables, formatted notes)
            if isinstance(raw_value, str) and raw_value != value:
                fact_data["html_value"] = raw_value

            # Add context information (period and dimensions) directly to the fact
            # This makes it easier to understand what makes each fact unique
            if fact.context is not None:
                context = fact.context

                # Add period information
                period_info = {}
                if context.isInstantPeriod:
                    period_info["type"] = "instant"
                    period_info["instant"] = str(context.instantDatetime) if context.instantDatetime else None
                elif context.isStartEndPeriod:
                    period_info["type"] = "duration"
                    period_info["start_date"] = str(context.startDatetime) if context.startDatetime else None
                    period_info["end_date"] = str(context.endDatetime) if context.endDatetime else None
                elif context.isForeverPeriod:
                    period_info["type"] = "forever"

                fact_data["period"] = period_info

                # Add entity information from context
                if hasattr(context, 'entityIdentifier') and context.entityIdentifier:
                    fact_data["entity_scheme"] = context.entityIdentifier[0]
                    fact_data["entity_identifier"] = context.entityIdentifier[1]

                # Add dimension information
                dimensions = []
                if hasattr(context, 'qnameDims') and context.qnameDims:
                    for dim_qname, dim_value in context.qnameDims.items():
                        if dim_qname is None or dim_value is None:
                            continue

                        try:
                            dim_data = {
                                "axis": str(dim_qname),
                                "axis_name": dim_qname.localName if hasattr(dim_qname, 'localName') else str(dim_qname),
                                "type": "explicit" if hasattr(dim_value, 'memberQname') else "typed"
                            }

                            # Get the member value
                            if hasattr(dim_value, 'memberQname') and dim_value.memberQname is not None:
                                dim_data["member"] = str(dim_value.memberQname)
                                dim_data["member_name"] = dim_value.memberQname.localName if hasattr(dim_value.memberQname, 'localName') else str(dim_value.memberQname)

                                # Try to get human-readable label for the dimension member
                                if hasattr(dim_value, 'member') and dim_value.member is not None:
                                    try:
                                        member_label = dim_value.member.label(lang="en-US")
                                        if member_label:
                                            dim_data["member_label"] = member_label
                                    except Exception as e:
                                        parse_log.log_info(
                                            "facts", e,
                                            concept=str(fact.qname),
                                            context_ref=fact.contextID,
                                            field="dimension_member_label",
                                        )
                            elif hasattr(dim_value, 'typedMember'):
                                dim_data["value"] = str(dim_value.typedMember.stringValue) if hasattr(dim_value.typedMember, 'stringValue') else str(dim_value.typedMember)

                            dimensions.append(dim_data)
                        except Exception as e:
                            parse_log.log_error(
                                "facts", e,
                                concept=str(fact.qname),
                                context_ref=fact.contextID,
                                field="dimension",
                            )

                if dimensions:
                    fact_data["dimensions"] = dimensions

            # Add human-readable label from taxonomy
            # This is the label that appears in financial statements
            if fact.concept is not None:
                try:
                    # Try to get standard label (what appears in statements)
                    standard_label = fact.concept.label(lang="en-US")
                    if standard_label:
                        # Decode HTML entities in labels
                        fact_data["label"] = html.unescape(standard_label)

                    # Also try to get terser label if available (shorter version)
                    terse_label = fact.concept.label(preferredLabel="http://www.xbrl.org/2003/role/terseLabel", lang="en-US")
                    if terse_label and terse_label != standard_label:
                        # Decode HTML entities in labels
                        fact_data["terse_label"] = html.unescape(terse_label)
                except Exception as e:
                    parse_log.log_warning(
                        "facts", e,
                        concept=str(fact.qname),
                        context_ref=fact.contextID,
                        field="label",
                    )

            # Add numeric flag directly from Arelle
            fact_data["is_numeric"] = fact.isNumeric

            # Add numeric-specific attributes
            if fact.isNumeric:
                fact_data["decimals"] = fact.decimals if hasattr(fact, 'decimals') else None
                fact_data["precision"] = fact.precision if hasattr(fact, 'precision') else None

            # Add data type
            if fact.concept is not None and hasattr(fact.concept, 'type'):
                try:
                    if fact.concept.type is not None and hasattr(fact.concept.type, 'qname'):
                        fact_data["data_type"] = str(fact.concept.type.qname)
                except Exception as e:
                    parse_log.log_warning(
                        "facts", e,
                        concept=str(fact.qname),
                        context_ref=fact.contextID,
                        field="data_type",
                    )

            # Add iXBRL source tracing information (for Inline XBRL files)
            # This allows linking back to the exact location in the SEC filing
            try:
                if hasattr(fact, 'id') and fact.id:
                    fact_data["html_anchor_id"] = fact.id

                if hasattr(fact, 'sourceline') and fact.sourceline:
                    fact_data["source_line"] = fact.sourceline

                # Extract source filename from the fact's model document
                if hasattr(fact, 'modelDocument') and fact.modelDocument is not None:
                    if hasattr(fact.modelDocument, 'basename'):
                        fact_data["source_file"] = fact.modelDocument.basename
            except Exception as e:
                parse_log.log_info(
                    "facts", e,
                    concept=str(fact.qname),
                    context_ref=fact.contextID,
                    field="ixbrl_source",
                )

            self._validate_fact_data(fact, fact_data, parse_log, validation_counts)
            facts.append(fact_data)

        # Flush aggregated validation counts
        for (severity, message, field), count in validation_counts.items():
            parse_log.log_aggregate(
                severity, "facts", f"{count} facts: {message}",
                count=count, field=field,
            )

        parse_log.record_section_count("facts", len(facts))
        return facts

    def _validate_fact_data(
        self,
        fact: Any,
        fact_data: Dict[str, Any],
        parse_log: ParseLogger,
        validation_counts: dict[tuple, int],
    ) -> None:
        """Run proactive validation checks on a fact before it is appended.

        Purely observational — does not modify fact_data.
        ERROR checks are logged per-fact (rare, need detail).
        WARNING/INFO checks are aggregated into validation_counts and flushed
        by the caller after the fact loop.
        """
        concept = fact_data.get("concept", "unknown")
        context_ref = fact_data.get("context_ref")
        is_numeric = fact_data.get("is_numeric", False)
        is_nil = fact_data.get("is_nil", False)
        value = fact_data.get("value")

        # ── ERROR checks (data integrity compromised — per-fact detail) ──

        if is_numeric and not is_nil:
            if value is None or value == "":
                parse_log.log_error(
                    "facts",
                    "Numeric non-nil fact has no value",
                    concept=concept,
                    context_ref=context_ref,
                    field="value",
                )
            elif isinstance(value, str):
                try:
                    float(value.replace(",", ""))
                except (ValueError, TypeError):
                    parse_log.log_error(
                        "facts",
                        f"Numeric value not parseable as float: {value!r}",
                        concept=concept,
                        context_ref=context_ref,
                        field="value",
                    )

        period = fact_data.get("period")
        if period is not None and period == {}:
            parse_log.log_error(
                "facts",
                "Context present but period is indeterminate (empty)",
                concept=concept,
                context_ref=context_ref,
                field="period",
            )

        # ── WARNING checks (usable but degraded — aggregated) ──

        if fact.context is None:
            key = (Severity.WARNING, "Fact has no context (no period, entity, or dimensions)", "context")
            validation_counts[key] = validation_counts.get(key, 0) + 1

        if is_numeric and not is_nil:
            if fact_data.get("unit_ref") is None:
                key = (Severity.WARNING, "Numeric fact missing unit_ref", "unit_ref")
                validation_counts[key] = validation_counts.get(key, 0) + 1
            if fact_data.get("decimals") is None and fact_data.get("precision") is None:
                key = (Severity.WARNING, "Numeric fact missing both decimals and precision", "decimals")
                validation_counts[key] = validation_counts.get(key, 0) + 1

        if period is not None:
            ptype = period.get("type")
            if ptype == "instant" and period.get("instant") is None:
                key = (Severity.WARNING, "Instant period with no date", "period")
                validation_counts[key] = validation_counts.get(key, 0) + 1
            elif ptype == "duration" and (period.get("start_date") is None or period.get("end_date") is None):
                key = (Severity.WARNING, "Duration period with missing start or end date", "period")
                validation_counts[key] = validation_counts.get(key, 0) + 1

        if fact.context is not None and fact_data.get("entity_identifier") is None:
            key = (Severity.WARNING, "Context present but no entity_identifier", "entity_identifier")
            validation_counts[key] = validation_counts.get(key, 0) + 1

        if not is_numeric and not is_nil and value == "":
            key = (Severity.WARNING, "Non-numeric, non-nil fact with empty string value", "value")
            validation_counts[key] = validation_counts.get(key, 0) + 1

        # ── INFO checks (enrichment gaps — aggregated) ──

        if fact_data.get("label") is None:
            key = (Severity.INFO, "No standard label resolved", "label")
            validation_counts[key] = validation_counts.get(key, 0) + 1

        if (
            fact_data.get("html_anchor_id") is None
            and fact_data.get("source_line") is None
            and fact_data.get("source_file") is None
        ):
            key = (Severity.INFO, "No iXBRL source info (anchor, line, file all missing)", "ixbrl_source")
            validation_counts[key] = validation_counts.get(key, 0) + 1

    def _generate_summary(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> Dict[str, Any]:
        """Generate summary statistics about the XBRL document.

        Uses actual extracted counts from parse_log when available, falling
        back to raw Arelle model counts for the null logger path.
        """
        # Extract unique namespaces from facts
        namespaces = set()
        for fact in model_xbrl.facts:
            try:
                if fact.qname.namespaceURI:
                    prefix = fact.qname.prefix
                    if prefix:
                        namespaces.add(prefix)
            except Exception:
                continue

        counts = getattr(parse_log, '_section_counts', {})
        return {
            "total_facts": counts.get("facts", len(model_xbrl.facts)),
            "total_contexts": counts.get("contexts", len(model_xbrl.contexts)),
            "total_units": counts.get("units", len(model_xbrl.units)),
            "namespaces": sorted(list(namespaces))
        }

    def _extract_concepts(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """
        Extract all concepts from the taxonomy.

        Concepts are the building blocks of XBRL - they define what can be reported
        (e.g., Assets, Revenue, Cash, etc.).

        Returns:
            List of concept dictionaries with labels, types, and properties
        """
        concepts = []

        logger.info(f"Extracting {len(model_xbrl.qnameConcepts)} concepts from taxonomy")

        for qname, concept in model_xbrl.qnameConcepts.items():
            try:
                concept_data = {
                    # Identification
                    "qname": str(qname),
                    "local_name": qname.localName,
                    "namespace_uri": qname.namespaceURI,
                    "prefix": qname.prefix if hasattr(qname, 'prefix') else None,

                    # Labels
                    "standard_label": None,
                    "terse_label": None,
                    "verbose_label": None,
                    "documentation": None,

                    # Type Information
                    "data_type": None,
                    "base_xsd_type": None,
                    "is_numeric": concept.isNumeric if hasattr(concept, 'isNumeric') else False,
                    "is_monetary": concept.isMonetary if hasattr(concept, 'isMonetary') else False,

                    # Financial Properties
                    "balance": concept.balance if hasattr(concept, 'balance') else None,
                    "period_type": concept.periodType if hasattr(concept, 'periodType') else None,

                    # Structure
                    "is_abstract": concept.isAbstract if hasattr(concept, 'isAbstract') else False,
                    "substitution_group": None,
                }

                # Extract labels (wrapped in try-except since labels might not exist)
                try:
                    standard_label = concept.label(lang="en-US")
                    if standard_label:
                        concept_data["standard_label"] = html.unescape(standard_label)
                except Exception as e:
                    parse_log.log_warning("concepts", e, concept=str(qname), field="standard_label")

                try:
                    terse_label = concept.label(preferredLabel=XbrlConst.terseLabel, lang="en-US")
                    if terse_label:
                        concept_data["terse_label"] = html.unescape(terse_label)
                except Exception as e:
                    parse_log.log_warning("concepts", e, concept=str(qname), field="terse_label")

                try:
                    verbose_label = concept.label(preferredLabel=XbrlConst.verboseLabel, lang="en-US")
                    if verbose_label:
                        concept_data["verbose_label"] = html.unescape(verbose_label)
                except Exception as e:
                    parse_log.log_warning("concepts", e, concept=str(qname), field="verbose_label")

                try:
                    documentation = concept.label(preferredLabel=XbrlConst.documentationLabel, lang="en-US")
                    if documentation:
                        concept_data["documentation"] = html.unescape(documentation)
                except Exception as e:
                    parse_log.log_warning("concepts", e, concept=str(qname), field="documentation")

                # Extract type information
                try:
                    if hasattr(concept, 'typeQname') and concept.typeQname:
                        concept_data["data_type"] = str(concept.typeQname)
                except Exception as e:
                    parse_log.log_warning("concepts", e, concept=str(qname), field="data_type")

                try:
                    if hasattr(concept, 'baseXsdType') and concept.baseXsdType:
                        concept_data["base_xsd_type"] = concept.baseXsdType
                except Exception as e:
                    parse_log.log_info("concepts", e, concept=str(qname), field="base_xsd_type")

                try:
                    if hasattr(concept, 'substitutionGroupQname') and concept.substitutionGroupQname:
                        concept_data["substitution_group"] = str(concept.substitutionGroupQname)
                except Exception as e:
                    parse_log.log_info("concepts", e, concept=str(qname), field="substitution_group")

                concepts.append(concept_data)

            except Exception as e:
                logger.debug(f"Error extracting concept {qname}: {e}")
                parse_log.log_error("concepts", e, concept=str(qname))
                continue

        logger.info(f"Successfully extracted {len(concepts)} concepts")
        parse_log.record_section_count("concepts", len(concepts))
        return concepts

    def _extract_labels(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """
        Extract all labels from the label linkbase.

        Labels are per-filing overrides of concept names. Companies customize
        labels in their extension taxonomy (e.g., "Net Sales" vs "Revenue").

        Returns:
            List of label dictionaries with concept_qname, label_role, label_text, language
        """
        label_rel_set = model_xbrl.relationshipSet(XbrlConst.conceptLabel)
        labels = []

        for qname, concept in model_xbrl.qnameConcepts.items():
            try:
                for rel in label_rel_set.fromModelObject(concept):
                    label_resource = rel.toModelObject
                    if label_resource is None or not label_resource.text:
                        continue
                    labels.append({
                        "concept_qname": str(qname),
                        "label_role": label_resource.role,
                        "label_text": strip_html(label_resource.text),
                        "language": label_resource.xmlLang or "en-US",
                    })
            except Exception as e:
                logger.debug(f"Error extracting labels for concept {qname}: {e}")
                parse_log.log_warning("labels", e, concept=str(qname))
                continue

        logger.info(f"Extracted {len(labels)} labels from label linkbase")
        parse_log.record_section_count("labels", len(labels))
        return labels

    def _extract_role_definitions(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """
        Extract all active role definitions from the presentation linkbase.

        Captures every role that has presentation relationships, with
        the parsed definition components (category, description) when the
        definition follows the SEC EDGAR ``NNNNNN - Category - Description``
        convention. Classification into statement types is deferred downstream.

        Returns:
            List of role definition dictionaries.
        """
        from sec_pipeline.config import parse_role_definition

        pres_rel_set = model_xbrl.relationshipSet(XbrlConst.parentChild)
        active_roles = {rel.linkrole for rel in pres_rel_set.modelRelationships}

        role_definitions = []

        for role_uri in sorted(active_roles):
            try:
                role_types = model_xbrl.roleTypes.get(role_uri, [])
                definition = role_types[0].definition if role_types and hasattr(role_types[0], 'definition') else None

                role_data = {
                    "role_uri": role_uri,
                    "definition": definition,
                    "category": None,
                    "description": None,
                }

                if definition:
                    parsed = parse_role_definition(definition)
                    if parsed is not None:
                        role_data["category"] = parsed[0]
                        role_data["description"] = parsed[1]

                role_definitions.append(role_data)

            except Exception as e:
                logger.debug(f"Error extracting role definition {role_uri}: {e}")
                parse_log.log_warning("role_definitions", e, detail=role_uri)
                continue

        logger.info(f"Successfully extracted {len(role_definitions)} role definitions")
        parse_log.record_section_count("role_definitions", len(role_definitions))
        return role_definitions

    def _extract_presentation_relationships(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """
        Extract presentation relationships (hierarchy) from the presentation linkbase.

        These define the visual structure and ordering of line items in financial statements
        (e.g., Assets contains Current Assets, which contains Cash, etc.).

        Iterates the flat modelRelationships directly to avoid exponential
        blowup from recursive tree traversal. Depth can be computed downstream
        via recursive CTE on parent_concept/child_concept/role_uri.

        Returns:
            List of presentation relationship dictionaries with parent-child edges
        """
        pres_rel_set = model_xbrl.relationshipSet(XbrlConst.parentChild)
        all_rels = pres_rel_set.modelRelationships

        logger.info(f"Extracting presentation relationships from {len(all_rels)} relationships")

        relationships = []

        for rel in all_rels:
            try:
                relationships.append({
                    "parent_concept": str(rel.fromModelObject.qname),
                    "child_concept": str(rel.toModelObject.qname),
                    "order": float(rel.order) if rel.order else None,
                    "preferred_label_role": rel.preferredLabel,
                    "role_uri": rel.linkrole,
                    "priority": rel.priority if hasattr(rel, 'priority') else None,
                })
            except Exception as e:
                logger.debug(f"Error extracting presentation relationship: {e}")
                parse_log.log_error("presentation_relationships", e)
                continue

        logger.info(f"Successfully extracted {len(relationships)} presentation relationships")
        parse_log.record_section_count("presentation_relationships", len(relationships))
        return relationships

    def _extract_calculation_relationships(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """
        Extract calculation relationships from the calculation linkbase.

        These define how line items sum together (e.g., Total Assets = Current Assets + Non-Current Assets).
        Each relationship has a weight (typically +1.0 or -1.0).

        Returns:
            List of calculation relationship dictionaries
        """
        # Get calculation relationship set — check both Calculations 1.0 (2003)
        # and Calculations 1.1 (2023) arcroles since newer filings use 1.1.
        calc_rel_set = model_xbrl.relationshipSet(XbrlConst.summationItem)
        all_rels = list(calc_rel_set.modelRelationships)

        if hasattr(XbrlConst, 'summationItem11'):
            calc_11_set = model_xbrl.relationshipSet(XbrlConst.summationItem11)
            all_rels.extend(calc_11_set.modelRelationships)

        logger.info(f"Extracting calculation relationships from {len(all_rels)} relationships")

        calculations = []

        # Iterate all calculation relationships
        for rel in all_rels:
            try:
                calc_data = {
                    "total_concept": str(rel.fromModelObject.qname),
                    "component_concept": str(rel.toModelObject.qname),
                    "weight": float(rel.weight) if hasattr(rel, 'weight') and rel.weight else None,
                    "order": float(rel.order) if hasattr(rel, 'order') and rel.order else None,
                    "role_uri": rel.linkrole if hasattr(rel, 'linkrole') else None,
                    "priority": rel.priority if hasattr(rel, 'priority') else None
                }
                calculations.append(calc_data)

            except Exception as e:
                logger.debug(f"Error extracting calculation relationship: {e}")
                parse_log.log_error("calculation_relationships", e)
                continue

        logger.info(f"Successfully extracted {len(calculations)} calculation relationships")
        parse_log.record_section_count("calculation_relationships", len(calculations))
        return calculations

    def _traverse_domain_member_tree(
        self, rel_set, concept, role_uri: str, depth: int = 0, visited: set = None
    ) -> List[Dict[str, Any]]:
        """
        Recursively traverse domain-member hierarchy.

        Args:
            rel_set: ModelRelationshipSet for domain-member relationships
            concept: Current domain/member concept
            role_uri: Role URI to filter by
            depth: Current depth (0 = domain root)
            visited: Set of visited concepts (cycle detection)

        Returns:
            List of domain-member relationship dicts
        """
        if visited is None:
            visited = set()

        concept_key = str(concept.qname)
        if concept_key in visited:
            return []

        visited.add(concept_key)
        results = []

        child_rels = rel_set.fromModelObject(concept)
        for rel in child_rels:
            if rel.linkrole != role_uri:
                continue
            child = rel.toModelObject

            results.append({
                "from_concept": str(concept.qname),
                "to_concept": str(child.qname),
                "relationship_type": "domain-member",
                "role_uri": role_uri,
                "order": float(rel.order) if rel.order else None,
                "depth": depth + 1,
                "priority": rel.priority if hasattr(rel, 'priority') else None,
            })

            results.extend(
                self._traverse_domain_member_tree(
                    rel_set, child, role_uri, depth + 1, visited.copy()
                )
            )

        return results

    def _extract_definition_relationships(self, model_xbrl: ModelXbrl, *, parse_log: ParseLogger = NULL_PARSE_LOGGER) -> List[Dict[str, Any]]:
        """
        Extract definition relationships from the definition linkbase.

        Covers all six arcrole types:
        - all / notAll: primary item <-> hypercube
        - hypercube-dimension: hypercube -> dimension
        - dimension-domain: dimension -> domain element
        - domain-member: domain -> members (hierarchical, uses tree traversal)
        - dimension-default: dimension -> default member

        Returns:
            List of definition relationship dicts
        """
        relationships = []

        # Flat arcrole types (simple iteration over modelRelationships)
        flat_arcroles = [
            (XbrlConst.all, "all"),
            (XbrlConst.notAll, "notAll"),
            (XbrlConst.hypercubeDimension, "hypercube-dimension"),
            (XbrlConst.dimensionDomain, "dimension-domain"),
            (XbrlConst.dimensionDefault, "dimension-default"),
        ]

        for arcrole_const, type_name in flat_arcroles:
            rel_set = model_xbrl.relationshipSet(arcrole_const)
            for rel in rel_set.modelRelationships:
                try:
                    rel_data = {
                        "from_concept": str(rel.fromModelObject.qname),
                        "to_concept": str(rel.toModelObject.qname),
                        "relationship_type": type_name,
                        "role_uri": rel.linkrole if hasattr(rel, 'linkrole') else None,
                        "order": float(rel.order) if hasattr(rel, 'order') and rel.order else None,
                        "priority": rel.priority if hasattr(rel, 'priority') else None,
                    }
                    # Capture closed attribute for all/notAll
                    if type_name in ("all", "notAll") and hasattr(rel, 'closed'):
                        rel_data["is_closed"] = str(rel.closed) if rel.closed else None
                    relationships.append(rel_data)
                except Exception as e:
                    logger.debug(f"Error extracting definition relationship ({type_name}): {e}")
                    parse_log.log_warning("definition_relationships", e, field=type_name)
                    continue

        # Domain-member arcrole (hierarchical, needs tree traversal)
        dm_rel_set = model_xbrl.relationshipSet(XbrlConst.domainMember)
        dm_total = len(dm_rel_set.modelRelationships)

        # Traverse from root concepts for each role
        root_concepts = dm_rel_set.rootConcepts if hasattr(dm_rel_set, 'rootConcepts') else []
        for root in root_concepts:
            try:
                # Determine which role(s) this root belongs to
                child_rels = dm_rel_set.fromModelObject(root)
                root_roles = set(rel.linkrole for rel in child_rels)
                for role_uri in root_roles:
                    relationships.extend(
                        self._traverse_domain_member_tree(
                            dm_rel_set, root, role_uri, depth=0
                        )
                    )
            except Exception as e:
                logger.debug(f"Error traversing domain-member tree from {root.qname}: {e}")
                parse_log.log_warning(
                    "definition_relationships", e,
                    concept=str(root.qname),
                    field="domain-member",
                )
                continue

        logger.info(
            f"Successfully extracted {len(relationships)} definition relationships "
            f"(domain-member source: {dm_total} raw rels)"
        )
        parse_log.record_section_count("definition_relationships", len(relationships))
        return relationships

