# Adversarial View Design — making Views fight the kernel

The goal is NOT to make another View. The goal is to make the View **hate** the kernel — to find every place where the kernel forces an unnatural implementation pattern.

For every View, ask three questions:

1. **What is the ugliest workaround?** Where does the View bend to fit the kernel?
2. **What operation is fundamentally expensive?** What costs more than it should?
3. **What is impossible?** Not difficult — impossible. That's the gold.

This is harder than expressiveness testing. "Can GraphView exist?" is easy. "Is GraphView forced into unnatural patterns because the kernel is missing something?" is the real question.

## Files

- `01_git_friction.md` — GitView friction analysis
- `02_sql_friction.md` — SQLView friction analysis
- `03_graph_friction.md` — GraphView friction analysis
- `04_streaming_friction.md` — StreamView friction analysis
- `05_vector_friction.md` — VectorView friction analysis
- `06_view_compression.py` — strip each View to irreducible translation layer, measure
- `07_translation_loss.md` — how much translation from native language to blobs?
- `SUMMARY.md` — what the friction tells us about the kernel

## Outcome vocabulary

For each friction point:
- **Kernel issue** — the kernel is missing something; admit a feature
- **View issue** — the View is poorly designed; fix the View
- **Acceptable tradeoff** — the friction is inherent to the workload
- **Falsification** — the kernel cannot express this; architecture fails

## The honest starting position

I don't know what I'll find. The previous phases proved expressiveness
(Views CAN exist). This phase tests whether they exist gracefully or
only through ugly workarounds. If every View bottoms out at 100-200
lines of irreducible translation, the kernel is good. If one View
bottoms out at 2500 lines, that's a kernel finding.
