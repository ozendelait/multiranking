#!/usr/bin/python
# coding=utf-8
from bs4 import BeautifulSoup
import requests, os, codecs, logging, json, time, datetime
from . import csv_tools as ct

crawler_kaggle_support = False
if 'KAGGLE_CONFIG_DIR' in os.environ:
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        from kaggle.api_client import ApiClient
        crawler_kaggle_support = True
    except:
        pass

if crawler_kaggle_support:
    # make sure to set the environment variable KAGGLE_CONFIG_DIR correctly and place kaggle.json there
    from kaggle.api.kaggle_api_extended import KaggleApi
    from kaggle.api_client import ApiClient
    import zipfile
    import io

    def extract_zip_inmemory(http_obj, extr_idx=0):
        filebytes = io.BytesIO(http_obj.data)
        zip_inmem = zipfile.ZipFile(filebytes)
        zip_names = list(sorted([name for name in zip_inmem.namelist()]))
        if len(zip_names) <= extr_idx:
            return None
        return zip_inmem.read(zip_names[extr_idx])

    def csv_to_json(inpcsv):
        all_subm = {}
        headers = []
        for idx,l in enumerate(inpcsv.split('\n')):
            vals = l.split(',')
            if len(vals) < 1 or len(vals[0]) < 1:
                continue
            if idx == 0:
                headers = [v[0].lower()+v[1:] for v in vals] #mimick call to view where first character gests changed to lower case
                continue
            else:
                append_dict = {headers[i]:v.replace('"','') for i,v in enumerate(vals)}
                if "submissionDate" in append_dict:
                    append_dict["submissionTs"] = time.mktime(datetime.datetime.strptime(append_dict["submissionDate"].replace('"',''), "%Y-%m-%d %H:%M:%S").timetuple())

                all_subm.setdefault(vals[0],[]).append(append_dict)
        #only keep newest submission per team:
        ret_subm = []
        for s in all_subm.values():
            if len(s) > 1:
                s = sorted(s, key=lambda x: x['submissionTs'], reverse=True)
            ret_subm.append(s[0])
        return {"submissions":ret_subm}

html_raw_postfix = "_raw"
csv_raw_postfix = "_raw"
column_id = "method"
rank_prefix = "rank"

logging.basicConfig(level=logging.INFO)
logger_name = __name__ + "combined"
rclogger = None
kaggle_api_obj = None


# initializes logging
def setup_logging(log_file):
    global rclogger, logger_name
    if rclogger is None:
        rclogger = logging.getLogger(logger_name)
        # log into file
        if not log_file is None:
            handler = logging.FileHandler(log_file)
            # create a logging format
            formatter = logging.Formatter('%(asctime)s -(%(filename)s:%(lineno)d- %(levelname)s: %(message)s')
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO) 
            # add the handlers to the logger
            rclogger.addHandler(handler)
    return rclogger


def save_html_dataset(url, trg_path, max_num_retries = 7):
    global kaggle_api_obj
    rclogger = setup_logging(None)
    call_kapi = url.startswith("kaggle://")
    for retry0 in range(max_num_retries):
        try:
            rclogger.info("Requesting online resource url " + url)
            if call_kapi:
                if kaggle_api_obj is None:
                    kaggle_api_obj = KaggleApi(ApiClient())
                    kaggle_api_obj.authenticate()
                resp_both = kaggle_api_obj.competition_download_leaderboard_with_http_info(id=url[len("kaggle://"):], _preload_content=False)
                resbytes = extract_zip_inmemory(resp_both[0])
                html_doc = resbytes.decode()
                if html_doc[0] == '\ufeff':  # remove BOM
                    html_doc = html_doc[1:]
                json_doc = csv_to_json(html_doc)
                resp = ct.clean_dict_utf8(json_doc)
                with open(trg_path,'w') as json_outp:
                   json.dump(resp, json_outp, ensure_ascii=False, indent=4)
                return True
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning) #hide ubiquitous SSl Warnings 
            html_doc = requests.get(url, verify=False, timeout=25.0).text
            break
        except Exception as e:
            if retry0 < max_num_retries-1:
                time.sleep(1.01) #prevent spamming of webservers
                continue
            rclogger.error("Failed to open url " + url + " Exception: " + str(e))
            return False
    try:
        with codecs.open(trg_path, 'w', encoding='utf8') as outfile:
            outfile.write(html_doc)
    except Exception as e:
        rclogger.error("Failed to save url " + url + " to file " + trg_path + "Exception: " + str(e))
        return False
    return True


def get_csv_datasets(src, url_id, html_path, csv_path, only_subset=True):
    rclogger = setup_logging(None)
    all_vals = {}
    try:
        soup = BeautifulSoup(open(html_path, 'rb'), 'html.parser', from_encoding='utf-8')
        all_rows = src.get_rows(soup)
    except Exception as e:
        excp_str = str(e)
        if len(excp_str) > 128:
            excp_str = excp_str[:128]+'...'
        rclogger.error("Could not load html file for parsing " + html_path + "; Exception: " + excp_str)
        return all_vals
    ranking_name = src.rank_prefix + "_" + url_id
    rowidx = 2
    add_rankings = src.needs_sortings(url_id)
    existing_ranking = src.has_ranking(url_id) or len(add_rankings) > 0

    if all_rows is None:
        all_vals = src.get_values(soup, url_id)
    else:
        for idx, row in enumerate(all_rows):
            try:
                vals = src.get_values(row, url_id, idx)
                if not existing_ranking:
                    vals[ranking_name] = rowidx
                if not column_id in vals:
                    rclogger.warning("Found empty id row at %s row %i" % (url_id, rowidx))
                    continue
                column_id_fixed = vals[column_id]
                if column_id_fixed in all_vals:
                    rclogger.warning("Skipping additional data for multiple entries of id " + column_id_fixed + " in file " + html_path)
                    continue
                all_vals[column_id_fixed] = vals
                rowidx += 1
            except ValueError:
                pass  # deliberately skip this row
            except Exception as e:
                rclogger.error("Parsing error for row %i of file %s Exception: %s" % (idx, html_path, str(e)))
                continue
    
    if len(all_vals) > 0:
        # calculate additional rankings
        for column_name, reverse_sorting in add_rankings:
            try:
                all_vals = src.add_rankings(all_vals, column_name, url_id, reverse_sorting=reverse_sorting)
            except Exception as e:
                rclogger.error("Error creating ranking for %s Exception: %s" % (column_name, str(e)))
                continue
        
        subset_all = all_vals.keys()
        header_list = ct.get_headers_from_keys(src, column_id, ct.get_all_keys(all_vals), url_id, only_subset)
        ct.save_as_csv(header_list, subset_all, all_vals, csv_path, column_id=column_id)
    else:
        rclogger.error("Could not extract any data from file " + html_path)


def fetch_datasets(all_sources, tmp_dir, only_subset=False):
    if len(all_sources) <= 0:
        return
    iter_src = all_sources
    if not type(all_sources[0]) is tuple:
        iter_src = [("all", all_sources)]
    sucess_subsets = []
    for name, sources in iter_src:
        sucess_fetch_all = True
        for src in sources:
            for url_id, url in src.all_urls(use_subset=only_subset).items():
                html_path = os.path.join(tmp_dir, url_id + html_raw_postfix + ".html")
                sucess_fetch_all &= save_html_dataset(url, html_path)
        if sucess_fetch_all:
            sucess_subsets.append(name)
    return sucess_subsets


def get_joined_dataset(all_sources, tmp_dir, only_subset=True, read_only=0, allow_fuzzy_namecmp=0, renaming_methods={}):
    rclogger = setup_logging(None)
    # The following steps are done in this sequence to later allow reconstruction of historical data; also some datasources might directly provide csv               
    if read_only <= 1:       
        # convert all html sources to csv, save to disk
        # PERFTODO: this could be done in parallel 
        for src in all_sources:
            for url_id, _ in src.all_urls(use_subset=only_subset).items():
                # html_doc = urllib2.urlopen(url).read()
                html_path = os.path.join(tmp_dir, url_id + html_raw_postfix + ".html")
                csv_path = os.path.join(tmp_dir, url_id + csv_raw_postfix + ".csv")
                get_csv_datasets(src, url_id, html_path, csv_path, only_subset)
    
    # load data from all csv and join data by "method" row (== unique identifier)
    loaded_urls = []
    for src in all_sources:
        src_urls = src.all_urls(use_subset=only_subset)
        for url_id, _ in src_urls.items():
            csv_path = os.path.join(tmp_dir, url_id + csv_raw_postfix + ".csv")
            loaded_urls.append(csv_path)
    
    return ct.join_csv_files(loaded_urls, column_id=column_id, rclogger=rclogger, allow_fuzzy_namecmp=allow_fuzzy_namecmp, renaming_methods=renaming_methods)


def calc_weight_per_benchmark(all_rankings, all_srcs, multiplier=100):
    cnt_rankings = {}
    max_id = None
    for src in all_srcs:
        cnt_rankings[src.name()] = 0
        for r in all_rankings:
            if r.find(src.name()) >= 0:
                cnt_rankings[src.name()] += 1
        if max_id is None or cnt_rankings[max_id] < cnt_rankings[src.name()]:
            max_id = src.name()
    
    # max_id gets 1 vote
    wheight_per_benchmark = {}
    for src_name, cnt in cnt_rankings.items():
        if cnt == 0:
            continue
        score_each = int(float(cnt_rankings[max_id] * multiplier) / float(cnt) + 0.5)
        for r in all_rankings:
            if r.find(src_name) >= 0:
                wheight_per_benchmark[r] = score_each
    return wheight_per_benchmark


def get_all_rankings(all_vals, required_name=None):
    all_keys = set()
    all_rankings = set()
    for _, vals in all_vals.items():
        keys = vals.keys()
        all_keys = all_keys | set(keys)
        for k in keys:
            if k.startswith(rank_prefix) and (required_name is None or k.find(required_name) >= 0):
                all_rankings.add(k)
    all_rankings = sorted(list(all_rankings))
    return all_keys, all_rankings


def remove_incomplete(all_vals, check_list, whitelist, return_incompletes=False):
    filtered_vals = {}
    incomplete_vals = {}
    for method, vals in all_vals.items():
        if not whitelist is None and not method in whitelist:
            continue
        all_entries_found = True
        for entry in check_list:
            if not entry in vals or vals[entry] is None or vals[entry] == "":
                all_entries_found = False
                break
        if all_entries_found:
            filtered_vals[method] = vals
        else:
            incomplete_vals[method] = vals
    if return_incompletes:
        return filtered_vals, incomplete_vals
    else:
        return filtered_vals


def mixed_ranking_vals_headers(all_vals, all_rankings, rank_first=True):
    rclogger = setup_logging(None)
    return_headers = []
    for r in all_rankings:
        if not r in all_vals:
            continue
        
        val_name = r.replace(rank_prefix + "_", '')
        
        if rank_first:
            return_headers.append(r)
            
        if val_name in all_vals:
            return_headers.append(val_name)
        else:
            rclogger.error("Value for rank " + r + "not found; adding without val")
            
        if not rank_first:
            return_headers.append(r)
            
    return return_headers


def normalize_rankings(all_vals, all_rankings, add_old_rank=False):
    rclogger = setup_logging(None)
    for r_name in all_rankings:
        calc_sort = []
        for name, vals in all_vals.items():
            if r_name in vals:
                calc_sort.append((int(vals[r_name]), name))
        if len(calc_sort) <= 0:
            continue
        calc_sort = sorted(calc_sort)
        prev_val = None
        prev_rank = 0
        for (r_old, name) in calc_sort:
            if r_old != prev_val:
                prev_rank += 1
            
            if add_old_rank:
                all_vals[name][r_name] = "%i (%i)" % (prev_rank, r_old)    
            else: 
                all_vals[name][r_name] = prev_rank       
    return all_vals

            
def add_new_ranking(all_vals, column_id, ranking_name, ranking):
    curr_rank = 1
    col_id_to_key = {vals[column_id]:key0 for key0, vals in all_vals.items() if column_id in vals} #support fuzzy mixed case comparisions
    for same_rank in ranking:
        if isinstance(same_rank,str):
            same_rank = [same_rank]
        for r in same_rank:
            if not r in all_vals and r in col_id_to_key:
                r = col_id_to_key[r]
            if not r in all_vals:
                all_vals[r] = {column_id:r}
            all_vals[r][ranking_name] = curr_rank
        curr_rank += len(same_rank)
    return all_vals
