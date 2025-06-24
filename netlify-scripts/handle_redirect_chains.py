import pandas as pd
from utils import add_path_placeholder
from natsort import natsorted
from generate_netlify_redirects import write_to_csv

def find_double_hops(redirects_list: list)-> set:
    redirect_origins = set()
    redirect_destinations = set()
    hops = []

    for redirect in redirects_list: 
        redirect_origins.add(redirect[0])
        redirect_destinations.add(redirect[1])
    hops = redirect_origins.intersection(redirect_destinations)
    return hops

    
def find_final_destination(original_destination: str, redirects_dict: dict):
    seen = set()
    current_destination = original_destination
    ## loop over redirect dict as long as current path is a key in the dict
    while current_destination in redirects_dict:
        if current_destination in seen:
            raise ValueError(f"Circular redirect detected at {current_destination}")
        seen.add(current_destination)
        ## Update current destination to be the value of the key of this dest
        current_destination = redirects_dict[current_destination]
    ## When loop breaks, it's because the current destination is not a key
    final_destination = current_destination
    return final_destination
    
## finds AND eliminates redirect chains
def find_and_eliminate_redirect_chains(redirects_list: list[tuple]) -> list[tuple]:
    print(len(redirects_list))
    # convert list of tuple redirects into dict format keyed by origin
    redirects_dict = {}
    circular_redirects = []
    for origin, destination in redirects_list:
        ##USE THIS ONLY when need to replace all values in redirects with another
        # if len(origin.split("/"))>= 5 and origin.split("/")[4] == "stable":
        #     origin = add_path_placeholder(origin, 4, "current")
        # if len(destination.split("/"))>= 5 and destination.split("/")[4] == "stable":
        #     destination = add_path_placeholder(destination, 4, "current")
        if origin in redirects_dict:
            print("DUPLICATE ORIGIN", origin, destination, redirects_dict[origin])
        redirects_dict[origin] = destination
    print(len(redirects_dict.items()))
    # Add all new redirects to list
    new_redirects = []
    # Make a copy of keys so that we can safely mutate the original dict
    for origin in list(redirects_dict.keys()):
        try:
            final = find_final_destination(redirects_dict[origin], redirects_dict)
            new_redirects.append((origin, final))
            if final != redirects_dict[origin]:
                circular_redirects.append((origin, redirects_dict[origin]))
        except ValueError as e:
            print(e)
        del redirects_dict[origin] 
    
    print("new redirects len", len(set(new_redirects)))
    print("redundant redirects", circular_redirects)
    return new_redirects, circular_redirects


def replace_path_section(redirect: tuple):
    redirect_origin = redirect[0]
    redirect_destination = redirect[1]
    branch = redirect_origin.split("/")[4]
    base_redirect_origin = redirect_origin.replace(branch, ':version')
    base_redirect_destination = redirect_destination.replace(branch, ':version')
    return base_redirect_origin, base_redirect_destination

def main(): 
    file_name = 'netlify-mongocli-wildcards-cleaned (3)'
    source_file_path = f'../netlify-redirects/{file_name}.csv'
    redirects_arr = pd.read_csv(source_file_path)
    redirects = list([*map(tuple,redirects_arr.values)])

    new_redirects, redirect_chains = find_and_eliminate_redirect_chains(redirects)
    redirects_sorted = natsorted(new_redirects, key=lambda x: x[0].casefold())
    print(set(redirects)-set(redirects_sorted))
    write_to_csv(redirects_sorted, f"{file_name}-cleaned")
    


if __name__ == "__main__":
    main()