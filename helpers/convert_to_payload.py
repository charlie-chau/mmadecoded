import csv
import json

fights = []
with open('input/20211120.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
        fight = {
            'fighter1_name': line[0].strip(),
            'fighter2_name': line[1].strip(),
            'fighter1_odds': float(line[2].strip()),
            'fighter2_odds': float(line[3].strip())
        }
        fights.append(fight)

print(json.dumps(fights, indent=4))
