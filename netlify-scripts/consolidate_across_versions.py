import re
import pandas as pd
from utils import add_path_placeholder, create_alias_dict, ensure_slashes, get_branch, add_placeholder, parse_raw_versions
from generate_netlify_redirects import write_to_csv

def normalize_path(path, skip_sections):
    return [part for part in path.strip("/").split("/")[skip_sections:] if part]


##Assumes that each path has a leading slash, otherwise index from 3
def is_equivalent_redirect(redirect_one: tuple, redirect_two: tuple, num_prefix_sections = 5):
    index = num_prefix_sections

    r1_origin = normalize_path(redirect_one[0], index)
    r1_dest = normalize_path(redirect_one[1], index)
    r2_origin = normalize_path(redirect_two[0], index)
    r2_dest = normalize_path(redirect_two[1], index)

    return r1_origin == r2_origin and r1_dest == r2_dest



def find_wildcards (redirect_list: str, main_versions: list, prefix: str, num_prefix_sections: int):
    partial_wildcards = {}
    wildcards = []

    i = 0
    while i < len(redirect_list):
        origin_branch = get_branch(redirect_list[i][0], num_prefix_sections)
        if origin_branch not in main_versions:
            i += 1
            continue
        original_redirect = redirect_list[i]
        base_origin = add_path_placeholder(original_redirect[0], num_prefix_sections, ":version")
        base_dest = add_path_placeholder(original_redirect[1], num_prefix_sections, ":version") if original_redirect[1].startswith(prefix) else original_redirect[1]
    
        # print(base_origin, base_dest)
        matches = []
        new_list = []
        for redirect in redirect_list:
            in_main = get_branch(redirect[0], num_prefix_sections) in main_versions
            if not in_main:
                print(get_branch(redirect[0], num_prefix_sections), "NOT IN MAIN")
            if in_main and is_equivalent_redirect((base_origin, base_dest), redirect, num_prefix_sections):
                matches.append(redirect)
            else:
                new_list.append(redirect)

        if len(matches) == len(main_versions):
            wildcards.append((base_origin, base_dest))
            # Replace with the unmatched redirects only
            redirect_list = new_list
            # Reset index to 0 since the list has changed
            i = 0
        elif len(matches) > 1:
            partial_wildcards[(base_origin, base_dest)] = len(matches)
            print(matches, len(matches))
            # Replace with the unmatched redirects only
            redirect_list = new_list
            # Reset index to 0 since the list has changed
            i = 0
        else: i+=1

    return wildcards


def remove_wildcard_caught_redirects(original_redirects_list: list, wildcards: set, main_versions: list, prefix: str, num_prefix_sections: int):
    remaining_redirects = []
    for redirect in original_redirects_list:
        placehold_redirect = (add_path_placeholder(redirect[0], num_prefix_sections, ":version"), add_path_placeholder(redirect[1], num_prefix_sections, ":version") if redirect[1].startswith(prefix) else redirect[1])
        # placehold_redirect = add_placeholder(redirect, num_prefix_sections, ":version")
        branch = get_branch(redirect[0], num_prefix_sections)
        # todo: OR non-numerical branch
        # print("Branch in get_branch:", get_branch(redirect[0]), "Origin:", redirect[0])
        if not placehold_redirect in wildcards and (branch in main_versions or not re.fullmatch(r'v\d+\.\d+', branch)):
           remaining_redirects.append(redirect)     
    return remaining_redirects


# takes a dictionary of versions and their aliases, replaces any aliases with the key
def clean_aliases(alias_dict, redirect_list, num_prefix_sections):
    cleaned_redirect_list = []
    for origin, destination in redirect_list:
        origin_version = origin.split("/")[num_prefix_sections]
        destination_version = destination.split("/")[num_prefix_sections]
        for key, alias_list in alias_dict.items():
            if origin_version in alias_list:
                origin = origin.replace(origin_version, key)
            if destination_version in alias_list:
                destination = destination.replace(destination_version, key)
        if origin!= destination:
            cleaned_redirect_list.append((origin, destination))
    return list(set(cleaned_redirect_list))

def main():
    file_name = 'netlify-java-rs-driver-redirects'
    source_file_path = f'../netlify-redirects/{file_name}.csv'
    redirects_arr = pd.read_csv(source_file_path)
    redirects = list([*map(tuple,redirects_arr.values)])
    print(len(redirects))
    ## Must use leading and trailing slash
    prefix = ensure_slashes("/docs/languages/java/reactive-streams-driver")

    raw_aliases =  """
main	upcoming
v5.5	current
v5.4	v5.4
v5.3	v5.3
v5.2	v5.2
v5.1	v5.1
v5.0	v5.0
    """

    alias_dict = create_alias_dict(raw_aliases)
    # alias_dict = {'current': ['v3.4'], 'upcoming': ['master']}
    main_versions = alias_dict.keys()
    print(alias_dict)


  
 

    num_prefix_sections = len(prefix.split("/"))-1 
    print("num prefix sections:", num_prefix_sections)
    new_redirect_list = clean_aliases( alias_dict, redirects, num_prefix_sections)
    print(len(set(new_redirect_list)))
    
    wildcards = find_wildcards(new_redirect_list, main_versions, prefix, num_prefix_sections)
    print(len(wildcards))
    #remove the ones associated with the wildcards
    remaining_redirects= remove_wildcard_caught_redirects(new_redirect_list, set(wildcards), main_versions, prefix, num_prefix_sections)
    print('\n\n', "remaining redirects that will be page levels:", len(remaining_redirects))
    write_to_csv(remaining_redirects, f"{file_name}-page-levels-2" )
    write_to_csv(wildcards, f"{file_name}-wildcards-2")
 

if __name__ == "__main__":
    main()