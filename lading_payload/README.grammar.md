# Grammar payload

Generates structured data from EBNF, PEG, or ANTLR v4 grammar files using
[barkus](https://github.com/DataDog/barkus). Each generated sample is a
newline-delimited byte sequence conforming to the grammar.

## Using it in a lading config

Reference the variant by name with a `grammar_path` pointing at the grammar
file and a `format` field indicating the grammar type.

**TCP generator** (variant is a top-level field):

```yaml
generator:
  - tcp:
      seed: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
      addr: "127.0.0.1:8080"
      bytes_per_second: "10 MiB"
      maximum_prebuild_cache_size_bytes: "64 MiB"
      variant:
        grammar:
          grammar_path: "/path/to/json.ebnf"
          format: ebnf
          max_depth: 20
          max_total_nodes: 5000
```

**HTTP generator** (variant is nested under `method.post`):

```yaml
generator:
  - http:
      seed: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
             17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
      headers: {}
      target_uri: "http://127.0.0.1:8080/"
      bytes_per_second: "1 MiB"
      parallel_connections: 1
      method:
        post:
          maximum_prebuild_cache_size_bytes: "10 MiB"
          variant:
            grammar:
              grammar_path: "/path/to/sql.g4"
              format: antlr
```

## Configuration fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `grammar_path` | path | (required) | Path to the grammar file. Can be absolute or relative to the lading working directory. |
| `format` | string | (required) | Grammar format: `ebnf`, `peg`, or `antlr`. |
| `max_depth` | integer | 30 | Maximum recursion depth for the grammar expansion. Lower values produce smaller, simpler output. |
| `max_total_nodes` | integer | 20000 | Maximum AST nodes per generated sample. Limits the total size of each output. |

## Supported grammar formats

- **EBNF** (`format: ebnf`): ISO/IEC 14977 Extended Backus-Naur Form. Rules
  use `=` and terminate with `;`. Repetition with `{ }`, optional with `[ ]`.
- **PEG** (`format: peg`): Parsing Expression Grammars. Rules use `<-` or `=`.
  Ordered choice with `/`, quantifiers `?`, `*`, `+`.
- **ANTLR** (`format: antlr`): ANTLR v4 combined or parser grammars (`.g4`
  files). Rules use `:` and terminate with `;`. Supports `grammar Name;`
  headers and `fragment` rules.

## Example EBNF grammar (simplified JSON)

```ebnf
value = object | array | string | number | "true" | "false" | "null" ;

object = "{" [ pair { "," pair } ] "}" ;
pair = string ":" value ;

array = "[" [ value { "," value } ] "]" ;

string = '"' { character } '"' ;
character = "a" | "b" | "c" | "x" | "y" | "z" | "0" | "1" | "2" ;

number = [ "-" ] digit { digit } [ "." digit { digit } ] ;
digit = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" ;
```

This produces output like:

```
{"a1":true,"xz":[false,42,-7.3]}
"abc"
[null,{"y":0}]
```

## Tuning tips

- Start with the defaults (`max_depth: 30`, `max_total_nodes: 20000`). Lower
  `max_depth` if you want shallower output or faster generation.
- Increase `max_total_nodes` for grammars with many terminals per sample (e.g.,
  large SQL statements).
- If the grammar's start rule requires deep recursion, ensure `max_depth` is at
  least as large as the start rule's minimum depth. Lading validates this at
  startup and will report an error if the depth budget is too small.
