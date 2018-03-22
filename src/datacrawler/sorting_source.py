from collections import namedtuple
from datacrawler import column_id
from datacrawler import rank_prefix

class sorting_source_cl:
    #extracted from <td> elements from <tr> nodes taken from html tables
    column_id = column_id
    rank_prefix = rank_prefix
    
    class TDEntry():
        # name: name assigned for this property, if name == column_id, than the value is the unique identifier used to join multiple rankings; if  name starts with rank_prefix, than this represents an ordering used as input into joining for multi-rank
        # pos: <td>'s position within <tr> tr; if integer, this is an index, if str or list of str, than this is the matching class attribute of the respective <td>
        # ptype: property type, see source of sorting_source_cl.get_values for possible values
        # is_unique: property is unique for each all versions (different online resources of the same benchmark); e.g. runtime is usually the same value for all variations/metrics of one entry -> is_unique = False
        # needs_ranking: framework needs to calculate a ranking for this property 0: no property needed; 1: add ranking, sort ascending (best values are low), -1: add ranking, sort descending (best values are high)
        def __init__(self, name, pos, ptype, is_unique=True, needs_ranking=0):
            self.name = name
            self.pos = pos
            self.ptype = ptype
            if name == column_id:
                self.is_unique = True
            else:
                self.is_unique = is_unique
            self.needs_ranking = needs_ranking    
    
    def name(self):
        return "name"
    #online resource base url
    def base_url(self):
        return "http://{url}"
    #these format substitutions for base_url define the full scope of the crawler; the respective online resources are saved to disk
    def formats(self):
        return {}
    #these format substitutions for base_url define the evaluation scope of the crawler; the respective saved resources are parsed for rankings
    def format_subset(self):
        return self.formats()
    
    def get_rows(self, soap):
        return []
    #these rankings are created by the framework (they are not found in the data), list of tuples [(name, reverse_order),...]
    def needs_sortings(self, version):
        all_sortings = []
        for tde in self.get_relevant_td(version):
            if tde.needs_ranking:
                all_sortings.append((tde.name, dir < 0))
        return all_sortings
    
    def get_relevant_td(self, version=""):
        return []
        
    def all_urls(self, use_subset=False):
        curr_urls = {self.name():self.base_url()}
        
        format_subset = self.formats()
        if use_subset:
            format_subset = self.format_subset()
        if format_subset is None:
            return {}
        params = sorted(list(format_subset.keys()))
        for i in range(len(params)):
            name = params[i]
            next_urls = {}
            for val in format_subset[name]:
                for old_id, url in curr_urls.iteritems():
                    next_urls[old_id+'-'+val] = url.replace("{"+name+"}",val)
            curr_urls = next_urls
        return curr_urls
        
    def get_values(self, soap1, version):
        #needed to fix a strange BeautifulSoup shortcoming
        def remove_tag(val, tag):
            td0 = val.find('<'+tag)
            if td0 >= 0:
                td0 = val.find('>', td0)
            td1 = val.find('</'+tag)
            if td1 > td0 and td0 >= 0:
                return val[td0+1:td1].strip()
            return val
    
        all_vals = {}
        if not soap1.find('td', class_ = 'results_sub') is None:
            raise ValueError('Skip invalid row') #skip whole row
        entries = soap1.find_all('td')
        if len(entries) <= 0:
            raise ValueError('Skip invalid row') #skip whole row
        for tde in self.get_relevant_td(version):
            val = None
            name = self.param_name(tde, version)
            sub_pos = 0
            if isinstance(tde.pos, float):
                sub_pos = tde.pos-int(tde.pos)
                tde.pos = int(tde.pos)
            if isinstance(tde.pos, int):
                if tde.pos < len(entries):
                    val = entries[tde.pos].string
                    if val is None:
                        val = remove_tag(str(entries[tde.pos]), "td")
                        val_br = val.split('<br/>')
                        if len(val_br) > 0:
                            sel_idx = min(int(len(val_br)*sub_pos+0.5), len(val_br)-1)
                            val = val_br[sel_idx]
                            val = remove_tag(val,"span")
            else:
                pos_all = tde.pos
                if not isinstance(tde.pos, list):
                    pos_all = [tde.pos]
                for tde.pos in pos_all: #allow multiple alternative classes per entry (e.g. for *-even *-odd)
                    tag0 = soap1.find('td', class_=tde.pos)
                    if not tag0 is None and not isinstance(tag0, list):
                        val = tag0.string
                        if not val is None:
                            break
                
            if val is None:
                raise Exception("Invalid row; could not find position "+str(tde.pos)+ "("+name+")") # this should not happen          
            
            #extract from within <strong> / <b> tags
            for tag in ["strong>", "b>"]:
                mstr = val.split(tag)
                if len(mstr) > 2 and mstr[1][-2:]=="</":
                    val = mstr[1][0:-2].strip()
                    break
            
            if tde.ptype == "percentage":
                val = float(val.replace(u"%",u""))*0.01
            elif tde.ptype == "time":
                try:
                    if val.find('min') >= 0:
                        val = float(val.replace(u"min",u""))* 60.0
                    elif val.find('ms') >= 0:
                        val = float(val.replace(u"ms",u"")) * 0.001
                    else:
                        val = float(val.replace(u"s",u""))
                except ValueError:
                    val = -1
            elif tde.ptype == "date-us6":
                val = '20'+val[6:8]+'-'+val[0:2]+'-'+val[3:5]
            elif tde.ptype == "integer":
                val = int(val)
            elif tde.ptype == "float":
                val = val.replace("px","")
                val = float(val)
            else:
                val = val.strip()
            #make column_id unique
            if name == self.column_id:
                #remove reference tags
                pos_br0 = val.rfind('[')
                pos_br1 = val.rfind(']')
                if pos_br0 > 0 and pos_br1 > pos_br0+1:
                    extr_id_str = val[pos_br0+1:pos_br1]
                    try:
                        if int(extr_id_str) >= 0  :
                            val = val[0:pos_br0].strip()
                    except:
                        pass
                #fix strange unicode chars
                val = val.replace(u'\u039c',"M") #greek M 
            all_vals[name] = val
        return all_vals
    
    def has_ranking(self, version):
        for tde in self.get_relevant_td(version):
            if tde.name.startswith(self.rank_prefix):
                return True
        return False
    
    def get_tde(self, check_name):
        for tde in self.get_relevant_td():
            if tde.name == check_name:
                return tde
        return None
    
    def param_name(self, tde, version):
        if tde.name == self.column_id:
            return tde.name
        if not tde.is_unique:
            version = self.name()
        if tde.name == rank_prefix:
            tdes = self.get_relevant_td(version)
            unique_entries = []
            for td1 in tdes:
                if td1.name == self.column_id or td1.name == rank_prefix or not td1.is_unique:
                    continue
                unique_entries.append(td1)
            if len(unique_entries) == 1:
                #there is exactly one unique entry -> rank is referencing this
                return rank_prefix + "_" + self.param_name(unique_entries[0], version)
            if len(unique_entries) == 0:
                #there is only a rank without values associated to it -> this rank represents the whole online resource/version
                return rank_prefix + "_" + version
        if tde.name.startswith(rank_prefix):
            return tde.name # extracted rankings are not touched; its better to generate rankings (using TDEntry.needs_ranking = True)
        return version + "_" + tde.name

    def add_rankings(self, all_vals, column_name, version, reverse_sorting = False):
        rankings = {}
        tde = self.get_tde(column_name)
        if tde is None:
            find_name = column_name
        else:
            find_name = self.param_name(tde, version)
        
        param_name = rank_prefix+"_"+find_name    
        rankings[find_name] = []
        for key,v in all_vals.iteritems():
            if find_name in v:
                rankings[find_name].append((key,v[find_name]))

        o_sort = sorted(rankings[find_name], key=lambda x: x[1], reverse = reverse_sorting)
        
        if len(o_sort) <= 0:
            raise Exception("Could not generating ranking. No elements for "+find_name+ " found.")  
        
        for idx,(k,v) in enumerate(o_sort): #TODO: allow parity of entries (same ranks for same values)
            all_vals[k][param_name] = (idx+1)
        return all_vals
            
            
