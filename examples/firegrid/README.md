# firegrid examples — promoted to `src/Firegrid.Grid/`

The ratified end-user API and its eight scenarios were promoted out of this
directory in Phase C / C1 (signature-identical move; annotations and
comments travel with the files):

- `Firegrid.fs` → [`src/Firegrid.Grid/Firegrid.Grid.fs`](../../src/Firegrid.Grid/Firegrid.Grid.fs)
  — the ratified contract (Tool / Agent / Session / Grid / TurnHandle + the
  model's choreography vocabulary: wait_for / wait_until / spawn / publish /
  execute), every member annotated with its platform lowering.
- `GridExamples.fs` → [`src/Firegrid.Grid/GridExamples.fs`](../../src/Firegrid.Grid/GridExamples.fs)
  — the eight ratified scenarios (the `Firegrid.Durable`/`Examples.fs`
  pattern: the examples file lives beside the contract it exercises).

The ratified text lives on in git history (this directory at commit
`717163f` and earlier); it is not forked. The T2 scenario corpus that drives
the surface green is `apps/proofs/GridLawProofs.fs` (suite `t2-firegrid`).
