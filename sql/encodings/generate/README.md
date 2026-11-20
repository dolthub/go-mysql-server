This package generates the embedded binary sort-weight tables and loader code
used by Unicode Collation Algorithm (UCA) collations. Embedding binary tables
keeps builds fast and lets collations load lazily on demand.

During generation, all UCA tables are normalized to one weight convention:
ignorable characters have weight zero and all other characters have positive
weights. Newer UCA 9.0.0 inputs already follow this convention, while older
UCA 4.0.0 and 5.2.0 inputs are shifted up by one so ignorable characters map
to zero without changing relative sort order.

The hand-maintained range lookups in the parent package's older UCA collation
files use the same convention and should stay aligned if regenerated.
