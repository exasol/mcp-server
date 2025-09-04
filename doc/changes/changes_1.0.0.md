# 1.0.0 - 2025-09-04
## Features

* #57: Added DB connection recovery.
* #60: Made meta queries case-insensitive in terms of input parameters.

## Refactoring

* #47: Re-written meta SQL queries using SQLGlot.
* #57: Replaced TF-IDF with BM25 in the keyword search.
* #61: Modified the keyword hint in the `find_xxx` tools' description.

## Dependency Updates

### `main`
* Added dependency `rank-bm25:0.2.2`
