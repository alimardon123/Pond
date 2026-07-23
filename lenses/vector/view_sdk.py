"""
Mock implementation of CrossLens, built ONLY from the SDK specification.

NOT the real SDK.  Used for testing.
"""


class CrossLens:
    """Static helpers for cross-view data access."""

    @staticmethod
    def read_from(lens, key: str):
        return lens.get(key)

    @staticmethod
    def read_all_from(lens) -> dict:
        return lens.get_all()

    @staticmethod
    def write_to(lens, key: str, data) -> str:
        return lens.put(key, data)

    @staticmethod
    def share_blob(from_view, from_key: str, to_view, to_key: str) -> bool:
        snapshot = from_view._get_snapshot()
        if from_key not in snapshot:
            return False
        to_view.put_raw(to_key, snapshot[from_key])
        return True

    @staticmethod
    def pipe(from_view, to_view, transformer=None) -> int:
        count = 0
        for key, data in from_view.get_all().items():
            if transformer is not None:
                data = transformer(data)
            to_view.put(key, data)
            count += 1
        return count
