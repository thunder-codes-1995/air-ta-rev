import platform
from dotenv import load_dotenv
import random

load_dotenv()

def create_consumer_id(prefix: str):
    host = platform.node()
    return f"{prefix}-{host}"


def add_randomness(approximate_time_in_secs, range_in_secs = 5):
    randomized = random.uniform(approximate_time_in_secs, range_in_secs)
    return randomized
