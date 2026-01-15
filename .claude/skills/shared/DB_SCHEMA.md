# db.yaml Schema Reference

Each skill maintains a `db.yaml` index and detailed files in `db/`.

## Index Entry

```yaml
entries:
  - target: <file:function>        # hunt/validate
    branch: <branch-name>          # review/rescue
    technique: <prealloc|buffer-reuse|etc>
    status: <success|failure|bug_found|approved|rejected>
    file: db/<name>.yaml
```

## Detail File

```yaml
target: <file:function>
technique: <technique>
status: <status>
date: <YYYY-MM-DD>
branch: <branch-name>
measurements:
  time: <-X% or ~>
  memory: <-X% or ~>
  allocations: <-X% or ~>
reason: |
  <why this outcome>
lessons: |
  <what was learned>
```

Adapt fields as needed. Include `bug_description` for bugs, `duplicate_of` for duplicates, `votes` for reviews.
