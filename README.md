# CLI for parsing PyPI code

## Installation

Grab a copy of the compiled binary for your system from [the latest GitHub release](https://github.com/pypi-data/cli/releases/tag/latest) and download it.

Then, grab a copy of some PyPI code. The tool will automate this in the future, but for now you could use the following:

```bash
git clone https://github.com/pypi-data/pypi-mirror-118/
git clone https://github.com/pypi-data/pypi-mirror-119/
git clone https://github.com/pypi-data/pypi-mirror-220/
git clone https://github.com/pypi-data/pypi-mirror-221/
```

## Usage

The tool currently has two modes of operation: parsing and extracting.

### Extracting files

You can extract files that match a given glob expression from a directory of git repositories. The files will be copied 
to the output directory with their names set to their git OIDs. You can also optionally filter the contents of the 
files using a regular expression.

```bash
./pypi-data extract <path to git repos> <path to output directory> <glob expression> [--contents=<regex>]
```

For example, to extract all `pyproject.toml` files that contain `django` to `~/extracted_files/` from `~/pypi_data/`:

```bash
./pypi-data extract ~/pypi_data/ ~/extracted_files/ "*/pyproject.toml" --contents="django"
```

### Mass parsing

To simplify parsing, the tool can output a JSON object for each match

```shell
./pypi-data parse <path to git repos> <glob expression> [--contents=<regex>]
```

For example, to parse every Python file that contains the string `django` in `~/pypi_data/`:

```shell
./pypi-data parse ~/pypi_data/ "*.py" --contents="django" | python parse.py
```

With `parse.py` being:

```python
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
```