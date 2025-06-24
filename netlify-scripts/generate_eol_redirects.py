redirect_label = "[[redirect]]"
from_label = "from = "
to_label = "to = "
asterisk = "/*"
splat = "/:splat"

ok_statius_code = "status = 200"
temporary_status_code = "status = 302"

intermediary_label = 'intermediary/'



# returns array of minor versions between start and end numbers, inclusive
def create_numerical_versions_list(start: float, end:float):
    i = int(start*10)
    end = int(end * 10)
    print(i, end)
    versions_to_eol = []
    while i<=end:
        versions_to_eol.append(f"v{i // 10}.{i % 10:01d}")
        i= i + 1
    return versions_to_eol

def create_eol_redirect(prefix, destination, desired_dest):
    origin_path = f'{from_label}"{prefix}{destination}{asterisk}"'
    destination_path = f'{to_label}"{prefix}{desired_dest}{splat}"'

    eol_redirect = "\n".join([redirect_label, origin_path, destination_path])
    return eol_redirect

def create_eol_redirect_list(versions_to_eol, prefix, desired_dest, ascending_order: bool):
    eol_redirects = []
    if not ascending_order:
        versions_to_eol.reverse()

    for version in versions_to_eol:
        # pass in version again as 3rd param to have redirects redirect back to self
        eol_redirects.append(create_eol_redirect(prefix, version, desired_dest))

    return "\n \n".join(eol_redirects)

def create_intermediary_version_redirect(prefix, destination):
    origin_path = f'{from_label}"{prefix}{destination}{asterisk}"'
    intermediary_path_destination = f'{to_label}"{prefix}{intermediary_label}{destination}{splat}"'

    intermediary_path_origin = f'{from_label}"{prefix}{intermediary_label}{destination}{asterisk}"'
    destination_path = f'{to_label}"{prefix}{destination}"'

    first_redirect = "\n".join([redirect_label, origin_path, intermediary_path_destination])
    second_redirect = "\n".join([redirect_label, intermediary_path_origin, destination_path])
    intermediary_redirects = "\n\n".join([first_redirect, second_redirect])
    return intermediary_redirects

def create_intermediary_version_redirect_list(online_versions, prefix):
    intermediary_redirects = []

    for version in online_versions:
        # pass in version again as 3rd param to have redirects redirect back to self
        intermediary_redirects.append(create_intermediary_version_redirect(prefix, version))

    return "\n \n".join(intermediary_redirects)


def create_circular_redirect():
    return

def create_alias_redirect(prefix, key, alias_list):
    alias_redirects = []
    for alias in alias_list:
        origin = f'{from_label}"{prefix}{alias}"'
        destination = f'{to_label}"{prefix}{key}"'
        alias_redirects.append("\n".join([redirect_label, origin, destination, temporary_status_code]))
    return "\n\n".join(alias_redirects)


def create_alias_redirect_list(prefix, alias_dict):
    all_alias_redirects = []
    for key, alias_list in alias_dict.items():
        all_alias_redirects.append(create_alias_redirect(prefix, key, alias_list))
    return "\n\n".join(all_alias_redirects)



def main():
    ## Must use leading and trailing slash
    prefix = "/docs/kafka-connector/"

    # returns array of minor versions between start and end numbers, inclusive
    versions_to_eol =  create_numerical_versions_list(1.0, 1.9)
    desired_eol_dest = "current"
    ascending = True
    #TODO: account for edge cases
    eol_redirects_list = create_eol_redirect_list(versions_to_eol, prefix, desired_eol_dest, ascending)
    
    # dictionary of aliases keyed by slug
    # value is list of aliases for that slug
    alias_dict = {'v1.15': ['current'], 'upcoming': ['master']}
    all_alias_redirects = create_alias_redirect_list(prefix, alias_dict)
    print(all_alias_redirects)
    online_versions = ['v1.12', 'v1.13', 'v1.14', 'current', 'upcoming']
    intermediary_redirects = create_intermediary_version_redirect_list(online_versions, prefix)
    # print(intermediary_redirects)

    

if __name__ == "__main__":
    main()




