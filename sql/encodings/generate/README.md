This package generates the embedded binary sort-weight tables and loader code
used by Unicode Collation Algorithm (UCA) collations in the parent package.
Storing the weight tables as embedded binary data keeps compilation fast and
avoids loading unused collations into memory until they are needed.

UCA collations treat certain code points, such as zero-width spaces and control
characters, as ignorable during string comparison and hashing. The generator
normalizes all UCA weight tables to a single convention where ignorable code
points have weight zero and every non-ignorable character has a positive weight.
Newer UCA 9.0.0 tables already follow this convention. Older UCA 4.0.0 and
5.2.0 tables were extracted from single-character comparisons in which ignorable
code points tied with ASCII space, so the generator rewrites those non-space
entries to zero and shifts all remaining non-ignorable weights up by one to
preserve their relative order.

The hand-maintained range lookups in the parent package's older UCA collation
files follow the same convention (zero for ignorable ranges and non-ignorable
ranges shifted up by one) and should stay aligned if those tables are ever
regenerated.
