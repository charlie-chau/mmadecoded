from helpers.db_helper import *

DB = get_db()


def main():
    dirty_misc_stats = get_dirty_misc_stats(DB)

    for dirty_misc_stat in dirty_misc_stats:
        found_id = get_fighter_id(DB, None, dirty_misc_stat['fighter'])
        dirty_misc_stat['fighter_id'] = found_id
        print(dirty_misc_stat)
        upsert_misc_stats_on_id(DB, dirty_misc_stat)

    dirty_sig_strike_stats = get_dirty_sig_strike_stats(DB)

    # print('Found {} dirty sig strike stats'.format(str(len(list(dirty_sig_strike_stats)))))

    for dirty_sig_strike_stat in dirty_sig_strike_stats:
        found_id = get_fighter_id(DB, None, dirty_sig_strike_stat['fighter'])
        dirty_sig_strike_stat['fighter_id'] = found_id
        print(dirty_sig_strike_stat)
        upsert_sig_strike_stats_on_id(DB, dirty_sig_strike_stat)


if __name__ == "__main__":
    main()
