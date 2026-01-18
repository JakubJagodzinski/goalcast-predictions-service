import logging
from random import random


def predict_winner(home_team: str, away_team: str) -> str:
    logging.info(f"Predicting winner for {home_team} vs {away_team}")

    winner = home_team if random() > 0.5 else away_team

    logging.info(f"Winner is {winner}")

    return winner
