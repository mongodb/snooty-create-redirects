from utils import create_alias_dict, ensure_slashes, parse_raw_versions

redirect_label = "[[redirects]]"
from_label = "from = "
to_label = "to = "
asterisk = "/*"
splat = "/:splat"

ok_status_code = "status = 200"
temporary_status_code = "status = 302"

intermediary_label = 'intermediary/'



# returns array of minor versions between start and end numbers, inclusive
1.1 - 1.11
def create_numerical_versions_list(start: float, end:float):
    # get whatever is before decimal place in 'end'
    #get whatever is after decimal place in end
    #compare value in front and behind decimal place on each loop
    i = int(start*10)
    end = int(end * 10)
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

    first_redirect = "\n".join([redirect_label, origin_path, intermediary_path_destination, temporary_status_code])
    second_redirect = "\n".join([redirect_label, intermediary_path_origin, destination_path, temporary_status_code])
    intermediary_redirects = "\n\n".join([first_redirect, second_redirect])
    return intermediary_redirects

def create_intermediary_version_redirect_list(online_versions, prefix):
    intermediary_redirects = []

    for version in online_versions:
        # pass in version again as 3rd param to have redirects redirect back to self
        intermediary_redirects.append(create_intermediary_version_redirect(prefix, version))

    return "\n \n".join(intermediary_redirects)


def create_circular_redirect(prefix, version):
    origin = f'{from_label}"{prefix}{version}{asterisk}"'
    destination = f'{to_label}"{prefix}{version}{splat}"'
    circular_redirect = "\n".join([redirect_label, origin, destination, ok_status_code])
    return circular_redirect

def create_insert_slug_redirect(prefix, primary_version):
    origin = f'{from_label}"{prefix}{asterisk[1]}"'
    destination = f'{to_label}"{prefix}{primary_version}{splat}"'
    insert_slug_redirect = "\n".join([redirect_label, origin, destination])
    return insert_slug_redirect

    
def create_circular_redirect_list(prefix, online_versions, primary_version = 'current'):
    circular_redirects = []

    for version in online_versions:
        # pass in version again as 3rd param to have redirects redirect back to self
        circular_redirects.append(create_circular_redirect(prefix, version))

    primary_redirect = create_insert_slug_redirect(prefix, primary_version)
    circular_redirects.append(primary_redirect)
    return "\n \n".join(circular_redirects)

def create_alias_redirect(prefix, key, alias_list):
    alias_redirects = []
    for alias in alias_list:
        ## Only create an alias redirect for versions with an alias(es)
        if alias!= key:
            origin = f'{from_label}"{prefix}{alias}{asterisk}"'
            destination = f'{to_label}"{prefix}{key}{splat}"'
            alias_redirects.append("\n".join([redirect_label, origin, destination, temporary_status_code]))
    return "\n\n".join(alias_redirects)


def create_alias_redirect_list(prefix, alias_dict):
    all_alias_redirects = []
    for key, alias_list in alias_dict.items():
        all_alias_redirects.append(create_alias_redirect(prefix, key, alias_list))
    return "\n\n".join(all_alias_redirects)



def main():
    ## Must use leading and trailing slash
    dest_file_name = "netlify-php-library"
    DESTINATION_FILE = f"../netlify-redirects/{dest_file_name}-standards.toml"
    prefix = ensure_slashes("/docs/php-library/")

    
    final_str = []

    # returns array of minor versions between start and end numbers, inclusive
    # versions_to_eol =  create_numerical_versions_list(0.10, 1.24)
    raw_offline_versions = """
    v1.17
    v1.16
    v1.15
    v1.14
    v1.13
    v1.12
    v1.11
    v1.10
    v1.9
    v1.8
    v1.7
    v1.6
    v1.5
    v1.4
    v1.3
    v1.2
    """
    offline_versions = parse_raw_versions(raw_offline_versions)
    # offline_versions = ["v8.1"]

    # The version that we want all offlined versions to redirect to
    desired_eol_dest = "current"
    ascending = True
    #TODO: account for edge cases
    if len(offline_versions) > 0:
        offline_redirects_list = create_eol_redirect_list(offline_versions, prefix, desired_eol_dest, ascending)
        final_str.append("\n### OFFLINE REDIRECTS ")
        final_str.append(offline_redirects_list)

    # dictionary of aliases keyed by slug
    # value is list of aliases for that slug
    raw_aliases =  """
    master	upcoming
    v2.x	current
    v1.x	v1.x
    """
    alias_dict = create_alias_dict(raw_aliases)
    # alias_dict = {'current': ['v2.0'], 'upcoming': ['master']}
    final_str.append("\n### ALIAS REDIRECTS")
    all_alias_redirects = create_alias_redirect_list(prefix, alias_dict)
    final_str.append(all_alias_redirects)
 
    # online_versions = create_numerical_versions_list(1.9, 2.8) + ['current', 'upcoming']
    raw_versions = """
    upcoming
    current
    v1.x
    """
    online_versions = alias_dict.keys()
    # online_versions = ["upcoming", "current"]
    
    intermediary_redirects = create_intermediary_version_redirect_list(online_versions, prefix)
    final_str.append(f"\n#Online versions: {online_versions}")
    final_str.append(f"### CATCH ALLS (Redirects any {prefix} page that would've 404ed to the version's landing page)")
    final_str.append(intermediary_redirects)

    ## Created so that if someone tries to visit a versioned product without a slug, they'll be redirected appropriately
    final_str.append("\n### CATCH ALLS (add slug to paths without slug)")
    final_str.append(create_circular_redirect_list(prefix, online_versions))
    
    f = open(DESTINATION_FILE, "w+")
    f.write("\n".join(final_str))

    

if __name__ == "__main__":
    main()




