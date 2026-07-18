# Identity Destruction II — attacking the foundational assumptions

The first destruction phase tested the kernel against designed workloads.
It found no new issues. That's a red flag: when a research project stops
finding new issues, confirmation bias becomes the default failure mode.

Identity Destruction II attacks the assumptions the first phase didn't
question. Each experiment tries to falsify a foundational claim of the
architecture. Each ends in: **Supported**, **Falsified**, **Inconclusive**,
or **Needs larger-scale validation**.

## The experiments

| # | Question | File |
|---|---|---|
| 1 | Is Reference primitive? Or is namespace a View concern? | `01_reference.py` |
| 2 | Is the namespace model right? (name→hash vs alternatives) | `02_namespace.py` |
| 3 | Is the kernel an API or laws? (separate invariants from operations) | `03_laws_vs_apis.md` |
| 4 | Can names disappear? (deletion, reachability, GC implications) | `04_name_deletion.py` |
| 5 | Can references be CRDTs? (multi-writer/multi-region) | `05_crdt_references.md` |
| 6 | Can two namespaces overlap? Compose? Conflict? | `06_namespace_composition.py` |
| 7 | Is hash primitive? (alternatives: location, capability, content-query) | `07_hash.py` |
| 8 | Is immutability binary? (partial mutability, tiered immutability) | `08_immutability.md` |

## The honest starting position

I don't know the answers. The first destruction phase assumed Reference
and namespace were primitive. This phase tries to disprove that.

If Reference survives, the 3-primitive kernel is strengthened.
If Reference falls, the kernel shrinks to 2 primitives (Write + Read) and
namespace becomes a View — a smaller, more interesting architecture.

Either outcome is progress.
