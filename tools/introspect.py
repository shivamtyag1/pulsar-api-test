"""Step 1 - dump the real schema for every endpoint.

The written docs are thin, so this is the highest-value thing we can run.
Everything downstream should be corrected against what this produces
rather than against the documentation.

Output: schema/<endpoint>.json  (full introspection)
        schema/<endpoint>.txt   (readable field listing)
"""

import json
import pathlib
import sys
from typing import Any

from pulsar.client import ENDPOINTS, PulsarClient, PulsarError

OUT = pathlib.Path("schema")

INTROSPECTION = """
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    types {
      kind
      name
      description
      fields(includeDeprecated: true) {
        name
        description
        args { name type { ...TypeRef } }
        type { ...TypeRef }
        isDeprecated
      }
      inputFields { name type { ...TypeRef } }
      enumValues(includeDeprecated: true) { name }
    }
  }
}

fragment TypeRef on __Type {
  kind name
  ofType {
    kind name
    ofType {
      kind name
      ofType { kind name }
    }
  }
}
"""


def type_name(t: dict[str, Any] | None) -> str:
    """Unwrap NON_NULL / LIST wrappers into something readable."""
    if not t:
        return "?"
    kind, name = t.get("kind"), t.get("name")
    if name:
        return str(name)
    inner = type_name(t.get("ofType"))
    if kind == "NON_NULL":
        return f"{inner}!"
    if kind == "LIST":
        return f"[{inner}]"
    return inner


def summarise(schema: dict[str, Any]) -> str:
    lines = []
    root_query = (schema.get("queryType") or {}).get("name")
    root_mutation = (schema.get("mutationType") or {}).get("name")

    types = [
        t for t in schema.get("types", []) if not (t.get("name") or "").startswith("__")
    ]
    # Roots first, then object types, then inputs and enums.
    order = {root_query: 0, root_mutation: 1}
    types.sort(key=lambda t: (order.get(t.get("name"), 2), t.get("name") or ""))

    for t in types:
        fields = t.get("fields") or t.get("inputFields") or []
        enums = t.get("enumValues") or []
        if not fields and not enums:
            continue

        tag = ""
        if t["name"] == root_query:
            tag = "   <-- ROOT QUERY"
        elif t["name"] == root_mutation:
            tag = "   <-- ROOT MUTATION"

        lines.append(f"\n{t['kind']}  {t['name']}{tag}")
        if t.get("description"):
            lines.append(f"  # {t['description']}")

        for f in fields:
            args = f.get("args") or []
            arg_str = ""
            if args:
                arg_str = (
                    "("
                    + ", ".join(f"{a['name']}: {type_name(a['type'])}" for a in args)
                    + ")"
                )
            dep = "  [DEPRECATED]" if f.get("isDeprecated") else ""
            lines.append(f"    {f['name']}{arg_str}: {type_name(f['type'])}{dep}")

        for e in enums:
            lines.append(f"    | {e['name']}")

    return "\n".join(lines)


def main() -> int:
    OUT.mkdir(exist_ok=True)
    targets = sys.argv[1:] or list(ENDPOINTS)
    failures = 0

    for name in targets:
        print(f"\n=== {name} ({ENDPOINTS.get(name, name)})")
        try:
            with PulsarClient(name) as client:
                data = client.execute(INTROSPECTION)
        except PulsarError as exc:
            print(f"  FAILED: {exc}")
            failures += 1
            continue

        schema = data["__schema"]
        (OUT / f"{name}.json").write_text(json.dumps(data, indent=2))
        (OUT / f"{name}.txt").write_text(summarise(schema))

        n_types = len(
            [t for t in schema["types"] if not (t.get("name") or "").startswith("__")]
        )
        print(f"  OK - {n_types} types -> schema/{name}.txt")

    print("\nRead the .txt files before touching steps 02-04.")
    print("The docs are approximate; these are authoritative.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
