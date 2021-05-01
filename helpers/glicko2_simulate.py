from helpers.glicko2 import Rating, Glicko2

GLICKO2 = Glicko2()

fighter1_rating = Rating(mu=2280.90264304067, sigma=0.19942981713454389, phi=179.24985521550164)
fighter2_rating = Rating(mu=2194.593008561904, sigma=128.59022288946562, phi=0.19912902414509182)

new_fighter1_rating, new_fighter2_rating = GLICKO2.rate_1vs1(fighter1_rating, fighter2_rating, 1, False)

print('Fighter 1 new rating: {}'.format(new_fighter1_rating))
print('Fighter 2 new rating: {}'.format(new_fighter2_rating))