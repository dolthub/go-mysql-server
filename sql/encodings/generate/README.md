This standalone package generates `../weightmaps.go` and the embedded `../*_Weights.bin` lazy-loaded weight maps for UCA collations.

Weight Convention:
- Ignorable UCA code points map to `IgnorableWeight` (`0`).
- UCA 9.0.0 (`*_0900_*`) input maps in this directory already store `0` for ignorable code points and positive weights (`>= 1`) for non-ignorable code points.
- `PAD SPACE` UCA 4.0.0 and 5.2.0 input maps in this directory were originally extracted via single-character `STRCMP` probes where ignorable code points shared the ASCII space weight. `normalizeUCAMaps` maps those non-`Zs` space-weight runes to `0` and shifts all remaining non-ignorable weights below `math.MaxInt32` up by `+1`.
- The sequential and constant range branches in the checked-in `../*.go` `RuneWeight` files for `PAD SPACE` UCA collations use the same `+1` offset convention (`0` for ignorable ranges, `+1` for non-ignorable ranges below `math.MaxInt32`). Keep those range branches aligned with `+1` if re-extracting or editing `../*.go`.
