# scripts/run_agents.py
import argparse, json, os
from agents.utils import read_json, write_json
from common.llm import LLMClient
from agents.a1_requirements import run_a1
from agents.a3_schema import run_a3
from agents.a4_load_mapping import run_a4

def main():
    ap = argparse.ArgumentParser(description="Run A1/A3/A4 agents")
    ap.add_argument("--provider", choices=["mock","openai"], default="mock")
    ap.add_argument("--model", default=None)
    ap.add_argument("--a1-free-text", default="", help="User free text for A1 (can be empty)")
    ap.add_argument("--profile", required=True, help="Path to A2 column_profile.json")
    ap.add_argument("--source", required=True, help="Original CSV path")
    ap.add_argument("--out-prefix", default="artifacts/museum", help="Prefix for outputs")
    args = ap.parse_args()

    client = LLMClient(provider=args.provider, model=args.model)

    profile = read_json(args.profile)
    file_meta = profile.get("file_meta") or {"path": args.source, "format": "csv", "detected": {}}

    # A1
    requirements = run_a1(client, args.a1_free_text, file_meta)
    write_json(f"{args.out_prefix}_requirements.json", requirements)

    # A3
    schema = run_a3(client, requirements, profile)
    write_json(f"{args.out_prefix}_schema.json", schema)

    # A4
    load_mapping = run_a4(client, requirements, profile, schema, args.source)
    write_json(f"{args.out_prefix}_load_mapping.json", load_mapping)

    print("Wrote:",
          f"{args.out_prefix}_requirements.json",
          f"{args.out_prefix}_schema.json",
          f"{args.out_prefix}_load_mapping.json", sep="\n")

if __name__ == "__main__":
    main()
