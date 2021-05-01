from helpers.request_helper import *
from helpers.sherdog_db_helper import *
import bson
from multiprocessing import Pool
import traceback

DB = get_db()


def get_snapshot_list():
    snapshots = get_all_mod_glicko_snapshot_no_count(DB)
    snapshot_list = []
    for snapshot in snapshots:
        snapshot_list.append(snapshot)

    return snapshot_list


def add_hist_count(snapshot):
    fighter_id = snapshot['fighter_id']

    glicko_hists = get_mod_glicko_history_fighter_all(DB, fighter_id)
    glicko_hists_list = list(glicko_hists)

    sorted_glicko_hist = sorted(glicko_hists_list, key=lambda x: x['date'], reverse=False)

    count = 0
    for glicko_hist in sorted_glicko_hist:
        count += 1
        glicko_hist['fight_count'] = count
        upsert_mod_glicko_history(DB, glicko_hist)

        # print(glicko_hist)

    snapshot['fighter_count'] = count
    print(snapshot)
    upsert_mod_glicko_snapshot(DB, snapshot)


if __name__ == '__main__':
    snapshot_list = get_snapshot_list()

    pool = Pool(processes=10)
    pool.map(add_hist_count, snapshot_list)
    pool.close()
