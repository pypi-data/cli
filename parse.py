import json, ast, sys

for line in sys.stdin.readlines():
    item = json.loads(line)
    path = item["path"]
    oid = item["oid"]
    try:
        ast.parse(item["contents"])
    except Exception:
        continue
    # Do something with the AST

    print(f"Parsed file {path} with OID {oid}")