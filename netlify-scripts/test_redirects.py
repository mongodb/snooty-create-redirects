import sys
import json
from utils import test_redirect



def prep_redirects_for_testing(redirects_list: list[tuple]) -> list[tuple]:
    rules = [
        (f"https://www.mongodb.com/{redirect[0]}", redirect[1])
        for redirect in redirects_list
    ]
    return rules


def test_all_redirects(redirects_list: list[tuple]) -> tuple:
    rules = prep_redirects_for_testing(redirects_list)
    fails = 0
    successes = 0
    for rule_from, rule_to in rules:
        status = test_redirect(rule_from, rule_to)
        if status:
            successes += 1
        else:
            fails += 1
    print(f"SUCCESSES: {successes}")
    print(f"FAILS: {fails}")

    return (successes, fails)


def main() -> None:
    source_file = sys.argv[1]

    with open(source_file, "r") as f:
        redirects = json.load(f)
    successes, failures = test_all_redirects(redirects)

    if failures > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
