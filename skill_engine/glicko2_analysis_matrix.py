import csv

import pandas as pd
import pymongo
from helpers.sherdog_db_helper import *

DB = get_db()


def get_fights_with_glicko(db, prob_field, prob, fight_count_min):
    fight_with_glicko_col = db['sherdog_fights_with_glicko2']

    found = fight_with_glicko_col.find(
        {
            '$or': [
                {
                    '$and': [
                        {
                            'fighter1_glicko2_info.' + prob_field: {
                                '$gte': prob
                            }
                        },
                        {
                            'fighter1_glicko2_info.' + prob_field: {
                                '$lt': prob + 0.1
                            }
                        },
                        {
                            'fighter1_glicko2_info.fight_count': {
                                '$gte': fight_count_min
                            }
                        },
                        {
                            'fighter2_glicko2_info.fight_count': {
                                '$gte': fight_count_min
                            }
                        }
                    ]
                },
                {
                    '$and': [
                        {
                            'fighter2_glicko2_info.' + prob_field: {
                                '$gte': prob
                            }
                        },
                        {
                            'fighter2_glicko2_info.' + prob_field: {
                                '$lt': prob + 0.1
                            }
                        },
                        {
                            'fighter2_glicko2_info.fight_count': {
                                '$gte': fight_count_min
                            }
                        },
                        {
                            'fighter1_glicko2_info.fight_count': {
                                '$gte': fight_count_min
                            }
                        }
                    ]
                }
            ]
        }
    )

    return found


def get_fights_with_glicko_win(db, prob_field, prob, fight_count_min):
    fight_with_glicko_col = db['sherdog_fights_with_glicko2']

    found = fight_with_glicko_col.find(
        {
            '$or': [
                {
                    '$and': [
                        {
                            'fighter1_glicko2_info.' + prob_field: {
                                '$gte': prob
                            }
                        },
                        {
                            'fighter1_glicko2_info.fight_count': {
                                '$gte': fight_count_min
                            }
                        },
                        {
                            'fighter1_glicko2_info.' + prob_field: {
                                '$lt': prob + 0.1
                            }
                        },
                        {
                            'fighter2_glicko2_info.fight_count': {
                                '$gte': fight_count_min
                            }
                        },
                        {
                            'fighter1_glicko2_info.result': 'WIN'
                        }
                    ]
                },
                {
                    '$and': [
                        {
                            'fighter2_glicko2_info.' + prob_field: {
                                '$gte': prob
                            }
                        },
                        {
                            'fighter2_glicko2_info.' + prob_field: {
                                '$lt': prob + 0.1
                            }
                        },
                        {
                            'fighter2_glicko2_info.fight_count': {
                                '$gte': fight_count_min
                            }
                        },
                        {
                            'fighter1_glicko2_info.fight_count': {
                                '$gte': fight_count_min
                            }
                        },
                        {
                            'fighter2_glicko2_info.result': 'WIN'
                        }
                    ]
                }
            ]
        }
    )

    return found


if __name__ == '__main__':
    prob_list = [0.5, 0.6, 0.7, 0.8, 0.9]
    fight_count_list = [0, 5, 10, 15, 20]
    prob_method = 'win_prob_2'

    data_rows = [prob_list]
    for prob in prob_list:
        data_cols = []
        for min_fight_count in fight_count_list:
            total_fights = get_fights_with_glicko(DB, prob_method, prob, min_fight_count)
            total_fights_len = len(list(total_fights))

            if total_fights_len > 0:
                fights_win = get_fights_with_glicko_win(DB, prob_method, prob, min_fight_count)
                fights_win_len = len(list(fights_win))
                fights_win_pct = round((fights_win_len / total_fights_len) * 100, 2)
                data_cols.append(fights_win_pct)
            else:
                data_cols.append(None)

        data_rows.append(data_cols)
        print('For probability: {} - {} --> {}'.format(str(prob), str(prob + 0.1), data_cols))

    with open("prob2_matrix.csv", "wb") as file:
        writer = csv.writer(file)
        writer.writerows(data_rows)
