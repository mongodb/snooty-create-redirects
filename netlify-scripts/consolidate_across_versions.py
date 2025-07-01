import re
import pandas as pd
from utils import get_branch, add_placeholder
from generate_netlify_redirects import write_to_csv


##Assumes that each path has a leading slash, otherwise index from 3
def is_equivalent_redirect(redirect_one: tuple, redirect_two: tuple):
    redirect_one_origin = redirect_one[0].split("/")[4:]
    redirect_one_destination = redirect_one[1].split("/")[4:]
    redirect_two_origin = redirect_two[0].split("/")[4:]
    redirect_two_destination = redirect_two[1].split("/")[4:]
    return redirect_one_origin == redirect_two_origin 



def find_wildcards (redirect_list: str, main_versions: list):
    partial_wildcards = {}
    wildcards = []

    i = 0
    while i < len(redirect_list):
        origin_branch = get_branch(redirect_list[i][0])
        if origin_branch not in main_versions:
            i += 1
            continue

        original_redirect = redirect_list[i]
        base_origin, base_dest = add_placeholder(original_redirect, 3, ":version")

        matches = []
        new_list = []
        for redirect in redirect_list:
            in_main = get_branch(redirect[0]) in main_versions
            if in_main and is_equivalent_redirect((base_origin, base_dest), redirect):
                matches.append(redirect)
            else:
                new_list.append(redirect)

        if len(matches) == len(main_versions):
            wildcards.append((base_origin, base_dest))
        elif len(matches) > 1:
            partial_wildcards[(base_origin, base_dest)] = len(matches)

        # Replace with the unmatched redirects only
        redirect_list = new_list
        # Reset index to 0 since the list has changed
        i = 0

    return wildcards


def remove_wildcard_caught_redirects(original_redirects_list: list, wildcards: set, main_versions: list):
    remaining_redirects = []
    for redirect in original_redirects_list:
        placehold_redirect = add_placeholder(redirect, 3, ":version")
        # destination = add_placeholder(redirect[1], 3, ":version")
        branch = get_branch(redirect[0])
        # todo: OR non-numerical branch
        # print("Branch in get_branch:", get_branch(redirect[0]), "Origin:", redirect[0])
        if not placehold_redirect in wildcards and (branch in main_versions or not re.fullmatch(r'v\d+\.\d+', branch)):
           remaining_redirects.append(redirect)     
    return remaining_redirects


# takes a dictionary of versions and their aliases, replaces any aliases with the key
def clean_aliases(prefix, alias_dict, redirect_list):
    cleaned_redirect_list = []
    num_prefix_sections = len(prefix.split("/"))
    for origin, destination in redirect_list:
        origin_version = origin.split("/")[num_prefix_sections-1]
        destination_version = destination.split("/")[num_prefix_sections-1]
        for key, alias_list in alias_dict.items():
            if origin_version in alias_list:
                origin = origin.replace(origin_version, key)
            if destination_version in alias_list:
                destination = destination.replace(destination_version, key)
        if origin!= destination:
            cleaned_redirect_list.append((origin, destination))
    return cleaned_redirect_list

def main():
    main_versions = ['current', 'v1.25', 'v1.26', 'v1.27', 'v1.28', 'v1.29', 'v1.30', 'v1.31', 'v1.32']

    file_name = 'netlify-kubernetes-operator-redirects'
    source_file_path = f'../netlify-redirects/{file_name}.csv'
    redirects_arr = pd.read_csv(source_file_path)
    redirects = list([*map(tuple,redirects_arr.values)])
    print(len(redirects))
     ## Must use leading and trailing slash
    prefix = "/docs/kubernetes-operator/"
    alias_dict = {'current': ['stable', 'v1.33'], 'upcoming': ['master', 'v1.34']}
    new_redirect_list = clean_aliases(prefix, alias_dict, redirects)
    # print(new_redirect_list)
    wildcards = find_wildcards(new_redirect_list, main_versions)
    print(len(wildcards))
    print ("\n")
    
    #remove the ones associated with the wildcards
    remaining_redirects= remove_wildcard_caught_redirects(new_redirect_list, set(wildcards), main_versions)
    print('\n\n', len(remaining_redirects))
    write_to_csv(remaining_redirects, f"{file_name}-page-levels" )
    write_to_csv(wildcards, f"{file_name}-wildcards")
 

if __name__ == "__main__":
    main()