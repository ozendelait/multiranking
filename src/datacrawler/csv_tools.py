import datetime, logging, io
logging.basicConfig(level=logging.INFO)

epoch = datetime.datetime.utcfromtimestamp(0)
def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0
def unix_time_now():
    return unix_time_millis(datetime.datetime.now())

def clean_val_utf8(inp_val, prev_endless=4):
    if prev_endless <= 0:
        return inp_val
    if isinstance(inp_val,dict):
        return clean_dict_utf8(inp_val, prev_endless-1)
    elif isinstance(inp_val,list):
        return [clean_val_utf8(v, prev_endless-1) for v in inp_val]
    elif isinstance(inp_val,str):
        return inp_val.encode("utf-8").decode("ascii", "backslashreplace")
    return inp_val
    
def clean_dict_utf8(inp_dict, prev_endless=4):
    if prev_endless <= 0:
        return inp_dict
    cleaned_dict = {}
    for key, val in inp_dict.items():
        cleaned_dict[key] = clean_val_utf8(val, prev_endless)
    return cleaned_dict

# this method requires a header row to work
def load_from_csv(csv_path, column_id="method", order_name=None, rclogger = None):
    if rclogger is None:
        rclogger = logging.getLogger(__name__)
    all_lines = []
    comment_char = "#"
    seperator_char = ','
    seperator_def = '"sep='
    headers = []
    all_vals = {}

    try:
        with io.open(csv_path, 'r', encoding="utf8", newline='\n') as inp_file:
            all_lines = inp_file.readlines()
    except Exception as e:
        rclogger.error("Could not read csv file " + csv_path + "Exception: "+ str(e))
        return all_vals
    
    parsed_header_line = False
    rowidx = 1
    for idx0, l in enumerate(all_lines):
        if not parsed_header_line:
            pos_sep_char = l.find(seperator_def)
            if l.startswith(comment_char):
                continue
            if pos_sep_char >= 0 and pos_sep_char < len(l) - 1:
                c0 = pos_sep_char + len(seperator_def)
                seperator_char = l[c0:c0 + 1]
                continue
            elif l.find(seperator_char) < 0 and l.find(';') >= 0:
                seperator_char = ';'#Excel often forgets to insert the proper separator char but uses ';' instead of ',' per default
                
            headers = [x.strip() for x in l.split(seperator_char)]
            if column_id not in headers:
                rclogger.error("Csv file " + csv_path + " does not contain the column id row " + column_id + ". Skipping the whole file.")
                return all_vals
            
            parsed_header_line = True
            continue
        if l.startswith(comment_char):
            continue
        vals = [x.strip() for x in l.split(seperator_char)]
        vals_maped = {}
        for idx, v in enumerate(vals):
            if idx >= len(headers):
                rclogger.warning("Csv file " + csv_path + " contains inconsistent header/data information in row %i" % idx0)
            if headers[idx] == "" and v == "":
                continue
            vals_maped[headers[idx]] = v
            
        if not order_name is None:
            vals_maped[order_name] = rowidx
        if not column_id in vals_maped:
            rclogger.warning("Csv file " + csv_path + " is missing column id at data row %i" % idx0)
            continue
        all_vals[vals_maped[column_id]] = vals_maped
        rowidx += 1
        
    if len(all_vals) <= 0:
        rclogger.warning("Csv file " + csv_path + " does not contain any data rows.")
    return all_vals

def get_all_keys(all_vals):
    all_keys = set()
    for _, vals in all_vals.items():
        all_keys |= set(vals.keys())
    return sorted(list(all_keys))

def get_headers_from_keys(src, column_id="method", keys=[], version = None, use_subset=True):
    headers = []
    # column_id is first
    if not column_id is None:
        headers.append(column_id)
    
    src_list = []
    # add all columns from the source last
    dataset_names = src.all_urls(use_subset=True).keys()
    if len(dataset_names) < 1:
        dataset_names = [src.name()]
    for tde in src.get_relevant_td():
        iter_versions = [version]
        if version is None:
            iter_versions = dataset_names
        for version_add in iter_versions:
            name = src.param_name(tde, version_add)
            if not name in headers and name not in src_list:
                src_list.append(name)
    
    # remainder (orderings, etc.) is in the middle
    for k in keys:
        if not k in src_list and not k in headers:
            headers.append(k)
    return headers + src_list

def save_as_csv(header_list, subset_ranking, all_vals, res_file, order_name=None, column_id="method"):
    seperator = ';'
    repl_seperator = '%3B'

    col_id_to_key = {vals[column_id]: key0 for key0, vals in all_vals.items() if column_id in vals}  # support fuzzy mixed case comparisions

    if not order_name is None and not order_name in header_list:
        header_list = [order_name] + header_list
    
    with io.open(res_file, 'w', encoding="utf8", newline='\n') as outfile:
        outfile.write('"sep=' + seperator + '"\n')
        for header in header_list:
            outfile.write('%s;' % str(header))
        outfile.write('\n')
        idx_m = 1
        for methods in subset_ranking:
            has_content = False
            if not isinstance(methods, list):
                methods = [methods]
            for method in methods:
                if not method in all_vals and method in col_id_to_key:
                    method = col_id_to_key[method]
                if not method in all_vals:
                    continue
                for entry in header_list:
                    val = ""
                    if entry == order_name:
                        val = str(idx_m)
                    elif entry in all_vals[method]:
                        val = all_vals[method][entry]
                    
                    try:
                        val = str(val)
                    except:
                        val = val.encode('latin-1','ignore')
                    val = val.replace(seperator, repl_seperator)
                    outfile.write(val+seperator)
                outfile.write('\n')
                has_content = True
            if has_content:
                idx_m += len(methods)


def res_name_fuzzy_cmp(id0, allow_fuzzy_namecmp):
    if allow_fuzzy_namecmp > 0:
        id0 = str(id0).lower()
        if allow_fuzzy_namecmp > 1:
            id0 = filter(str.isalnum, id0)
    return id0

#from https://stackoverflow.com/questions/10953189/count-lower-case-characters-in-a-string
def n_lower_chars(string):
    return sum(1 for c in str(string) if c.islower())
    
def join_csv_files(csv_paths, column_id, rclogger = None, allow_fuzzy_namecmp = 0):
    if rclogger is None:
        rclogger = logging.getLogger(__name__)
    
    all_vals = {}
    for csv_path in csv_paths:
        vals_csv = load_from_csv(csv_path, column_id= column_id, order_name=None, rclogger = rclogger)
        for id0_orig, vals in vals_csv.items():
            id0 = res_name_fuzzy_cmp(id0_orig,allow_fuzzy_namecmp)
            vals[column_id] = id0_orig
            if id0 in all_vals:
                for name, val in vals.items():
                    old_val = None
                    if name in all_vals[id0]:
                        old_val = all_vals[id0][name]
                    if not old_val is None and old_val != val:
                        if name == column_id and allow_fuzzy_namecmp > 0 and ((len(str(old_val)) > len(str(val))) or (n_lower_chars(old_val) < n_lower_chars(val))):
                            continue # method names math, old version contains upper/lower casing or more of the methods non-alphanum characters -> keep
                        rclogger.warning("Collision of values, replacing %s with %s for %s of %s" % (str(old_val), str(val), name, vals["method"]))
                    all_vals[id0][name] = val
            else:
                all_vals[id0] = vals
    return all_vals
