# WIP CLI for mass parsing

How to use:

### Download the unique Python files dataset

```shell
curl -L --remote-name-all $(curl -L "https://github.com/pypi-data/data/raw/main/links/only_python_files.txt")
```

### Clone all the git repositories

From [these instructions](https://py-code.org/download)

```shell
wget https://py-code.org/download.sh
chmod +x download.sh
./download.sh pypi_code
```

### Run the tool: search for regular expressions

```shell
cargo run --release -- grep --pattern=".*foobar.*" [unique_files.parquet] [path_to_pypi_code]
```

### Run the tool: Parse using libcst

This will parse the files using libcst and search for specific syntax usage.

```shell
cargo run --release -- walk-cst [unique_files.parquet] [path_to_pypi_code]
```
