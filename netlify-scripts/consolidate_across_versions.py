import pandas as pd
from utils import get_associated_manual_version_redirects, get_branch, add_placeholder
from generate_netlify_redirects import write_to_csv


##Assumes that each path has a leading slash, otherwise index from 3
def is_equivalent_redirect(redirect_one: tuple, redirect_two: tuple):
    redirect_one_origin = redirect_one[0].split("/")[4:]
    redirect_one_destination = redirect_one[1].split("/")[4:]
    redirect_two_origin = redirect_two[0].split("/")[4:]
    redirect_two_destination = redirect_two[1].split("/")[4:]
    return redirect_one_origin == redirect_two_origin 



def find_wildcards (list: str, main_versions: list):
    partial_wildcards = {}
    wildcards=[]
    i = 0
    print(len(list))
    while i < len(list):
        if get_branch(list[i][0]) in main_versions:
            original_redirect = list[i]
            num_redundancies = 0
            base_redirect_origin, base_redirect_destination = add_placeholder(list[i], 3, ":version")
            j = 0
            matches = []
            while j < len(list):
                ## Find all redirects that match version-agnostic redirect
                comparative_redirect = list[j]
                in_main_versions: bool = get_branch(comparative_redirect[0]) in main_versions
                equivalent_redirects: bool = is_equivalent_redirect((base_redirect_origin, base_redirect_destination),(comparative_redirect) )
                if in_main_versions and equivalent_redirects:
                    num_redundancies +=1
                    matches.append(comparative_redirect)
                    del list[j]
                else: j+=1
            # print(matches)
            if num_redundancies == len(main_versions):
                print('adding to wildcards')
                wildcards.append((base_redirect_origin, base_redirect_destination))
            elif num_redundancies > 1: 
                print(f"no wildcard created for {(base_redirect_origin, base_redirect_destination)}")
                partial_wildcards[(base_redirect_origin, base_redirect_destination)] = num_redundancies
            else: print("didn't find redundancies for ", original_redirect)
        i+=1
    return wildcards


def remove_wildcard_caught_redirects(original_redirects_list: list, wildcards: set):
    redirects = [(add_placeholder(redirect, 3, ":version")) for redirect in original_redirects_list]
    remaining_redirects = []
    for redirect in wildcards:
        if not redirect in wildcards:
           remaining_redirects.append(redirect)     
    return remaining_redirects


def main():
    main_versions = ['v1.15', 'v1.14', 'v1.13', 'v1.12']

    file_name = 'netlify-kafka-connector-redirects'
    source_file_path = f'../netlify-redirects/{file_name}.csv'
    redirects_arr = pd.read_csv(source_file_path)
    redirects = list([*map(tuple,redirects_arr.values)])
    wildcards = find_wildcards(redirects, main_versions)
    print(len(wildcards))
    print ("\n")
 
    #remove the ones associated with the wildcards
    print(remove_wildcard_caught_redirects(redirects, set(wildcards)))
    write_to_csv(wildcards, f"{file_name}-wildcards")
 

if __name__ == "__main__":
    main()