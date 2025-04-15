import sys
import requests
import json

def test_redirect(origin: str, destination: str)-> bool: 
    resp = requests.head(origin)
    if resp.status_code != 301:
        print(f"FAIL: {origin}")
        resp.close()
        return False
    elif resp.headers["Location"] != destination:
        print(f"FAIL: {origin} -> {origin}, found {resp.headers['Location']}")
        resp.close()
        return False
    else:
        print(f"SUCCESS: {origin} -> {origin}, found {resp.headers['Location']}")
        resp.close()
        return True
    
def prep_redirects_for_testing(redirects_list: list[tuple])-> list[tuple]:
    rules = [
            (f"https://www.mongodb.com/{redirect[0]}", redirect[1])
            for redirect in redirects_list
            ]
    return rules

def test_all_redirects(redirects_list: list[tuple])-> tuple:
    rules = prep_redirects_for_testing(redirects_list)
    fails = 0
    successes = 0
    for rule_from, rule_to in rules:
        status = test_redirect(rule_from, rule_to)
        if status:
            successes+=1
        else:
            fails+=1
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
