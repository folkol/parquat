# Parquat

![parquat icon, a cat sitting on a parquet floor](parquat.png)

[Parquet](https://parquet.apache.org) [cat](https://pubs.opengroup.org/onlinepubs/9699919799/utilities/cat.html).

Concatenates and prints parquet files using [Polars](https://docs.rs/polars/latest/polars/). (A command-line tool / CLI for viewing .parquet files.)

## Installation

```
$ git clone https://github.com/folkol/parquat.git
$ cd parquat
$ cargo install --path .
```

## Usage

```
$ pcat *.parquet
$ pcat --query 'SELECT `foo.bar` as BAZ FROM t WHERE `foo.bar` <> 1337' *.parquet
```

## Advanced

For more table formatting options, see [Polars Config](https://docs.rs/polars/latest/polars/#config-with-env-vars).

## TODO

- [ ] Add support for 'http-paths'?
- [ ] Add support for stdin?
