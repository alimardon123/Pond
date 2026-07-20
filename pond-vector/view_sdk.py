"""
Mock implementation of CrossView, built ONLY from the SDK specification.

NOT the real SDK.  Used for testing.
"""


class CrossView:
    """Static helpers for cross-view data access."""

    @staticmethod
    def read_from(view, key: str):
        return view.get(key)

    @staticmethod
    def read_all_from(view) -> dict:
        return view.get_all()

    @staticmethod
    def write_to(view, key: str, data) -> str:
        return view.put(key, data)

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
