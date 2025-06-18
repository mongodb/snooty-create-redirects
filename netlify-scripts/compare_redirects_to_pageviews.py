import pandas as pd
import numpy as np

REDIRECT_FILE = '../../../downloads/manual-5.0-redirects.csv'
PAGEVIEWS_FILE = '../../../downloads/manual-5.0-pageviews.csv'

DESTINATION_FILE_CSV = f"manual-5.0-redirect-pageviews.csv"


#  One time use script for Sarah lin to compare a list of redirect pairs with a list of page+pageview pairs and sort by 0 pageviews. 
#  Used during migration off of S3 for consolidating/eliminating unnecessary redirects
def get_count(key):
    return key[2]

def main() -> None:
    redirects: list[list[str]] = pd.read_csv(REDIRECT_FILE).values
    pageviews = pd.read_csv(PAGEVIEWS_FILE)
    
    pageviews_map = {}
    for page in pageviews.values:
        page_url = page[0]
        pageview_count = page[1]
       

        docs_index = page_url.find("/docs")
        stripped_page = page_url[docs_index:]
        pageviews_map.setdefault(stripped_page, 0)
        pageviews_map[stripped_page] = pageviews_map[stripped_page] + pageview_count

    new_redirects = []
    for redirect in redirects:
        if redirect[0] in list(pageviews_map.keys()):
            pageview_count = pageviews_map[redirect[0]]
        else: pageview_count = 0
        
        new_redirect = [redirect[0], redirect[1], pageview_count]
        new_redirects.append(new_redirect)

    new_redirects.sort(key= get_count)
    print(len(new_redirects))

    df = pd.DataFrame(new_redirects, columns = ['origin', 'destination', 'number of page views'])
    df.to_csv(DESTINATION_FILE_CSV, index= False)


if __name__ == "__main__":
    main()
