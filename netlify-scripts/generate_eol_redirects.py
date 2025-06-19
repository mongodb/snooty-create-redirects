redirect_label = "[[redirect]]"
from_label = "from = "
to_label = "to = "
asterisk = "/*"
splat = "/:splat"


def create_eol_redirect(prefix, destination, desired_dest):
    origin_path = f'{from_label}"{prefix}{destination}{asterisk}"'
    destination_path = f'{to_label}"{prefix}{desired_dest}{splat}"'
    eol_redirect = "\n".join([redirect_label, origin_path, destination_path])
    return eol_redirect

def main():
    ## Must use leading and trailing slash
    prefix = "/docs/mongocli/"
    versions_to_eol = ['1.0.0', '1.1.0', '1.2.0', '1.3.0', '1.4.0', '1.5', '1.6', '1.7', '1.8', '1.9', '1.10', '1.11', '1.12', '1.13', '1.14', '1.15']
    desired_dest = "current"
    all_redirects = []
    for version in versions_to_eol:
        all_redirects.append(create_eol_redirect(prefix, version, desired_dest))
    print("\n \n".join(all_redirects))
    

if __name__ == "__main__":
    main()