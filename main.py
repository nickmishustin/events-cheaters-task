from datetime import date
import tracemalloc
from eventswihoutcheatres import EventsWithoutCheaters


def display_memory_usage(snapshot, key_type='lineno'):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))

if __name__ == '__main__':
    tracemalloc.start()

    e = EventsWithoutCheaters(
        cheaters_path='cheaters.db',
        result_path='result.db',
        server_path='server.csv',
        client_path='client.csv',
        target_date=date(year=2021, month=3, day=7)
    )
    e.set_df()
    e.write_to_db()

    snapshot = tracemalloc.take_snapshot()
    display_memory_usage(snapshot)
