import sys
import boto3

def main() -> None:
    source_file = sys.argv[1]
    from_base = "https://www.mongodb.com"

    with open(source_file, "r") as f:
        rules = [(rule[0], rule[1]) for rule in (line.split(None, 1) for line in f.read().split("\n")) if rule]

    output_rules = []

    for rule_from, rule_to in rules:
        assert rule_from.startswith(from_base), rule_from

        rule_from = rule_from.replace(from_base, "")
        rule_from = rule_from.lstrip("/")
        if not rule_from.endswith("/index.html"):
            rule_from = rule_from.rstrip("/") + "/index.html"
        output_rules.append((rule_from, rule_to))
        print(f"raw: {rule_from} -> {rule_to}")

    s3 = boto3.session.Session().resource("s3").Bucket(sys.argv[2])

    for rule_from, rule_to in output_rules:
        obj = s3.Object(rule_from)
        obj.put(WebsiteRedirectLocation=rule_to)


if __name__ == "__main__":
    main()
