from bs4 import BeautifulSoup
import requests, os, codecs, logging
import csv_tools as ct

html_raw_postfix = "_raw"
csv_raw_postfix = "_raw"
column_id = "method"
rank_prefix = "rank"

logging.basicConfig(level=logging.INFO)
logger_name = __name__ + "combined"
rclogger = None

#initializes logging
def setup_logging(log_dir):
    global rclogger, logger_name
    if rclogger is None:
        rclogger = logging.getLogger(logger_name)
        #log into file
        if not log_dir is None:
            log_file = os.path.join(log_dir,'robcrawler.log')
            handler = logging.FileHandler(log_file)
            # create a logging format
            formatter = logging.Formatter('%(asctime)s -(%(filename)s:%(lineno)d- %(levelname)s: %(message)s')
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO) 
            # add the handlers to the logger
            rclogger.addHandler(handler)
    return rclogger

def save_html_dataset(url, trg_path):
    rclogger = setup_logging(None)
    try:
        rclogger.info("Requesting online resource url "+ url)
        html_doc = requests.get(url, verify=False, timeout=2.0).text
    except Exception as e:
        rclogger.error("Failed to open url "+ url + " Exception: "+ str(e))
        return False
    try:
        with codecs.open(trg_path, 'w', encoding='utf8') as outfile:
            outfile.write(html_doc)
    except Exception as e:
        rclogger.error("Failed to save url "+ url + " to file " + trg_path + "Exception: "+ str(e))
        return False
    return True

def get_csv_datasets(src, url_id, html_path, csv_path, only_subset=True ):
    rclogger = setup_logging(None)
    all_vals = {}
    try:
        soup = BeautifulSoup(open(html_path), 'html.parser')
        all_rows = src.get_rows(soup)
    except Exception as e:
        rclogger.error("Could not load html file for parsing " + html_path)
        return all_vals
    ranking_name = src.rank_prefix+"_"+url_id
    rowidx = 2
    add_rankings = src.needs_sortings(url_id)
    existing_ranking = src.has_ranking(url_id) or len(add_rankings) > 0

    if all_rows is None:
        all_vals = src.get_values(soup, url_id)
    else:
        for idx, row in enumerate(all_rows):
            try:
                vals = src.get_values(row, url_id)
                if not existing_ranking:
                    vals[ranking_name] = rowidx
                if not column_id in vals:
                    rclogger.warning("Found empty id row at %s row %i" % (url_id, rowidx))
                    continue
                if vals[column_id] in all_vals:
                    rclogger.warning("Skipping additional data for multiple entries of id " + vals[column_id] + " in file " + html_path)
                    continue
                all_vals[vals[column_id]] = vals
                rowidx += 1
            except ValueError:
                pass #deliberately skip this row
            except Exception as e:
                rclogger.error("Parsing error for row %i of file %s Exception: %s" % (idx, html_path, str(e)))
                continue
    
    if len(all_vals)> 0:
        #calculate additional rankings
        for column_name, reverse_sorting in add_rankings:
            try:
                all_vals = src.add_rankings(all_vals, column_name, url_id, reverse_sorting = reverse_sorting)
            except Exception as e:
                rclogger.error("Error creating ranking for %s Exception: %s" % (column_name, str(e)))
                continue
        
        subset_all = all_vals.keys()
        header_list = ct.get_headers_from_keys(src, column_id, ct.get_all_keys(all_vals), url_id, only_subset)
        ct.save_as_csv(header_list, subset_all, all_vals, csv_path)
    else:
        rclogger.error("Could not extract any data from file " + html_path)

def fetch_datasets(all_sources,  tmp_dir, only_subset=False):
    if len(all_sources) <= 0:
        return
    iter_src = all_sources
    if not type(all_sources[0]) is tuple:
        iter_src = [("all", all_sources)]
    sucess_subsets = []
    for name, sources in iter_src:
        sucess_fetch_all = True
        for src in sources:
            for url_id, url in src.all_urls(use_subset=only_subset).iteritems():
                html_path = os.path.join(tmp_dir, url_id+html_raw_postfix+".html")
                sucess_fetch_all &= save_html_dataset(url, html_path)
        if sucess_fetch_all:
            sucess_subsets.append(name)
    return sucess_subsets


def get_joined_dataset(all_sources, tmp_dir, only_subset=True, read_only=0):
    rclogger = setup_logging(None)
    #The following steps are done in this sequence to later allow reconstruction of historical data; also some datasources might directly provide csv               
    if read_only <= 1:       
        # convert all html sources to csv, save to disk
        #PERFTODO: this could be done in parallel 
        for src in all_sources:
            for url_id, _ in src.all_urls(use_subset=only_subset).iteritems():
                #html_doc = urllib2.urlopen(url).read()
                html_path = os.path.join(tmp_dir, url_id+html_raw_postfix+".html")
                csv_path = os.path.join(tmp_dir, url_id+csv_raw_postfix+".csv")
                get_csv_datasets(src, url_id, html_path, csv_path, only_subset)
    
    #load data from all csv and join data by "method" row (== unique identifier)
    loaded_urls = []
    for src in all_sources:
        src_urls = src.all_urls(use_subset=only_subset)
        for url_id, _ in src_urls.iteritems():
            csv_path = os.path.join(tmp_dir, url_id+csv_raw_postfix+".csv")
            loaded_urls.append(csv_path)
    
    return ct.join_csv_files(loaded_urls, column_id= column_id, rclogger = rclogger)

def calc_weight_per_benchmark(all_rankings, all_srcs, multiplier = 100):
    cnt_rankings = {}
    max_id = None
    for src in all_srcs:
        cnt_rankings[src.name()] = 0
        for r in all_rankings:
            if r.find(src.name()) >= 0:
                cnt_rankings[src.name()] += 1
        if max_id is None or cnt_rankings[max_id] < cnt_rankings[src.name()]:
            max_id = src.name()
    
    #max_id gets 1 vote
    wheight_per_benchmark = {}
    for src_name,cnt in cnt_rankings.iteritems():
        if cnt == 0:
            continue
        score_each = int(float(cnt_rankings[max_id]*multiplier)/float(cnt)+0.5)
        for r in all_rankings:
            if r.find(src_name) >= 0:
                wheight_per_benchmark[r] = score_each
    return wheight_per_benchmark

def get_all_rankings(all_vals, required_name = None):
    all_keys = set()
    all_rankings = set()
    for _, vals in all_vals.iteritems():
        keys = vals.keys()
        all_keys = all_keys | set(keys)
        for k in keys:
            if k.startswith(rank_prefix) and (required_name is None or k.find(required_name) >= 0):
                all_rankings.add(k)
    all_rankings = sorted(list(all_rankings))
    return all_keys, all_rankings

def remove_incomplete(all_vals, check_list, whitelist):
    filtered_vals = {}
    for method, vals in all_vals.iteritems():
        if not whitelist is None and not method in whitelist:
            continue
        all_entries_found = True
        for entry in check_list:
            if not entry in vals or vals[entry] is None or vals[entry] == "":
                all_entries_found = False
                break
        if all_entries_found:
            filtered_vals[method] = vals

    return filtered_vals

def add_new_ranking(all_vals, column_id, ranking_name, ranking):
    curr_rank = 1
    for same_rank in ranking:
        for r in same_rank:
            if not r in all_vals:
                all_vals[r] = {column_id:r}
            all_vals[r][ranking_name] = curr_rank
        curr_rank += len(same_rank)
    return all_vals