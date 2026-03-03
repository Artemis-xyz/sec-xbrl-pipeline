"""Quick smoke test: fetch latest MSFT 10-Q, parse it, dump summary + parse_log."""

import asyncio
import json
import sys

from sec_pipeline import SECAPIClient, XBRLParserService


async def main():
    client = SECAPIClient(
        user_agent_name="SEC Pipeline Test",
        user_agent_email="test@example.com",
    )
    parser = XBRLParserService(
        user_agent_name="SEC Pipeline Test",
        user_agent_email="test@example.com",
    )

    response = await client.get_company_filings("MSFT")
    ten_qs = [f for f in response.filings if f.form_type == "10-Q" and f.xbrl_instance_url]
    if not ten_qs:
        print("No 10-Q filings found")
        sys.exit(1)

    latest = ten_qs[0]
    print(f"Filing: {latest.accession_number} ({latest.filing_date})")
    print(f"URL:    {latest.xbrl_instance_url}")
    print()

    xbrl_data = await parser.parse_xbrl_from_url(latest.xbrl_instance_url)

    # Section counts
    print("=== Section Counts ===")
    for key, val in xbrl_data.items():
        if key == "parse_log":
            continue
        if isinstance(val, list):
            print(f"  {key}: {len(val)} items")
        elif isinstance(val, dict):
            print(f"  {key}: {json.dumps(val, indent=2, default=str)[:200]}")
        else:
            print(f"  {key}: {val}")

    # Parse log
    print()
    print("=== Parse Log ===")
    print(json.dumps(xbrl_data["parse_log"], indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(main())
