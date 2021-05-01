from helpers.sherdog_db_helper import *
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.calibration import calibration_curve
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import json
import matplotlib.transforms as mtransforms

DB = get_db()
organisation = None
# organisation = 'UFC'
after_date = None
# after_date = '2019-01-01'


def run():
    if organisation is not None and after_date is not None:
        events = get_event_ids_for_organisation(DB, organisation, after_date)
        fights = list(get_fights_with_glicko(DB, events))
    elif organisation is not None and after_date is None:
        events = get_event_ids_for_organisation(DB, organisation)
        fights = list(get_fights_with_glicko(DB, events))
    elif organisation is None and after_date is not None:
        events = get_event_ids_after_date(DB, after_date)
        fights = list(get_fights_with_glicko(DB, events))
    else:
        fights = list(get_fights_with_glicko(DB))

    print('Found {} fights.'.format(len(fights)))
    prob = 'win_prob_1_5'

    preds = [fight['fighter1_glicko2_info'][prob] for fight in fights] + [fight['fighter2_glicko2_info'][prob] for fight in fights]
    results = [fight['fighter1_glicko2_info']['result'] for fight in fights] + [fight['fighter2_glicko2_info']['result'] for fight in fights]
    results_binary = [fight['fighter1_glicko2_info']['result'] == "WIN" for fight in fights] + [fight['fighter2_glicko2_info']['result'] == "WIN" for fight in fights]

    brier_score_decomposition(fights, preds, prob)

    brier_score = brier_score_loss(results_binary, preds, pos_label=True)

    print('Brier Score Loss: ' + str(brier_score))

    cross_entropy = log_loss(results_binary, preds)

    print('Cross Entropy Loss: ' + str(cross_entropy))
    #
    glicko_y, glicko_x = calibration_curve(results_binary, preds, n_bins=100)

    fig, ax = plt.subplots()
    # calibration line
    plt.plot(glicko_x, glicko_y, marker='o', linewidth=1, label='glicko2')
    # reference line, legends, and axis labels
    line = mlines.Line2D([0, 1], [0, 1], color='black')
    transform = ax.transAxes
    line.set_transform(transform)
    ax.add_line(line)
    fig.suptitle('Calibration plot for Glicko2 probabilities')
    ax.set_xlabel('Predicted probability')
    ax.set_ylabel('WIN probability in each bin')
    plt.legend()
    plt.show()


def brier_score_decomposition(fights, preds, prob):
    calibration = {}
    base_rate = 0.5
    uncertainty = base_rate * (1-base_rate)
    reliability = 0
    resolution = 0

    for x in range(0, 100):
        bucket = [x/100, (x+1)/100]

        bucket_probs = []
        fights_in_bucket_count = 0
        wins_in_bucket_count = 0

        for fight in fights:
            if bucket[0] < fight['fighter1_glicko2_info'][prob] <= bucket[1]:
                fights_in_bucket_count = fights_in_bucket_count + 1
                bucket_probs.append(fight['fighter1_glicko2_info'][prob])
                if fight['fighter1_glicko2_info']['result'] == 'WIN':
                    wins_in_bucket_count = wins_in_bucket_count + 1

            if bucket[0] < fight['fighter2_glicko2_info'][prob] <= bucket[1]:
                fights_in_bucket_count = fights_in_bucket_count + 1
                bucket_probs.append(fight['fighter2_glicko2_info'][prob])
                if fight['fighter2_glicko2_info']['result'] == 'WIN':
                    wins_in_bucket_count = wins_in_bucket_count + 1

        if fights_in_bucket_count == 0:
            continue

        cal = wins_in_bucket_count / fights_in_bucket_count

        bucket_average_prob = sum(bucket_probs) / len(bucket_probs)

        # print('Bucket average prob for {0}: {1}'.format(str(bucket[0]) + " - " + str(bucket[1]), str(bucket_average_prob)))

        bucket_reliability = fights_in_bucket_count * ((cal - bucket_average_prob)**2)
        bucket_resolution = fights_in_bucket_count * ((cal - base_rate)**2)
        reliability = reliability + bucket_reliability
        resolution = resolution + bucket_resolution

        calibration[str(bucket[0]) + " - " + str(bucket[1])] = cal

    reliability = reliability / len(preds)
    resolution = resolution / len(preds)

    print('uncertainty: {}'.format(uncertainty))
    print('reliability: {}'.format(reliability))
    print('resolution: {}'.format(resolution))

    brier_charlie = uncertainty - resolution + reliability

    print('Brier A La Charlie: {}'.format(brier_charlie))


if __name__ == '__main__':
    run()
