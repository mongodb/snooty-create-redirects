import sys
import requests

def main() -> None:
    source_file = sys.argv[1]

    with open(source_file, "r") as f:
        rules = [(rule[0], rule[1]) for rule in (line.split(None, 1) for line in f.read().split("\n")) if rule]

    fails = 0
    successes = 0

    for rule_from, rule_to in rules:
        resp = requests.head(rule_from)
        if resp.status_code != 301:
            print(f"FAIL: {rule_from}")
            fails += 1
        elif resp.headers["Location"] != rule_to:
            print(f"FAIL: {rule_from}")
            fails += 1
        else:
            print(f"SUCCESS: {rule_from} -> {rule_to}")
            successes += 1

        resp.close()

    print(f"SUCCESSES: {successes}")
    print(f"FAILS: {fails}")

    if fails > 0:
        sys.exit(1)
