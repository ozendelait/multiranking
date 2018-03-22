#Hint: escape forward slashes (char '/') using %2F; otherwise the automatic naming for html/csv files will fail
from sorting_source import sorting_source_cl
import json

class sorting_sintel_flow(sorting_source_cl):
    def base_url(self):
        return "http://sintel.is.tue.mpg.de/quant?metric_id=0&selected_pass={pass}"
    def name(self):
        return "sintel_f"
    def formats(self):
        return {"pass" : {"0", "1"} }  # 0 == Final; 1 == Clean
    def get_rows(self, soup):
        all_tr = soup.find_all("table")[0].find_all("tr")[1:]
        return all_tr
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("EPEall", 1, "float", True, 1), self.TDEntry("EPEmatched", 2, "float", True, 1), 
                self.TDEntry("EPEunmatched", 3, "float", True, 1),  self.TDEntry("d0-10", 4, "float", True, 1),   self.TDEntry("d10-60", 5, "float", True, 1), 
                self.TDEntry("d60-140", 6, "float", True, 1), self.TDEntry("s0-10", 7, "float", True, 1), self.TDEntry("s10-40", 8, "float", True, 1), self.TDEntry("s40p", 9, "float", True, 1)] 
     
    
class sorting_middlb_flow(sorting_source_cl):
    def base_url(self):
        return "http://vision.middlebury.edu/flow/eval/results/results-{error_t}{stat}.php"
    def name(self):
        return "middlb_f"
    def formats(self):
        return {"error_t" : {"e", "a", "i", "n"},
                "stat" : {"1", "2", "3", "4", "5", "6", "7", "8"} }
    def format_subset(self):
        return {"error_t" : {"e"} ,
                "stat" : {"1", "2", "3", "4", "5", "6", "7", "8"}}
    def get_rows(self, soup):
        rows = []
        all_tr = soup.find_all("tr")
        first_idx = -1
        for idx, one_tr in enumerate(all_tr):
            js_cont = str(one_tr)
            if js_cont.find('<tr class="blue">') >= 0 or js_cont.find('<tr class="white">') >= 0:
                if first_idx < 0 :
                    first_idx = idx
                rows.append(one_tr)
            elif(first_idx >= 0):
                break
        return rows
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("avrg_rnk", 1, "float", True, 1)]  # TODO: add scores from images
        
class sorting_middlb_mvs(sorting_source_cl):
    def base_url(self):
        return "http://vision.middlebury.edu/mview/eval/pageDump.php"
    def name(self):
        return "middlb_mvs"
    def get_rows(self, soup):
        return None  # no standard <tr><td> schema but uses csv
    def needs_sortings(self, version):
        addrankings = []
        for ds in ["Temple","Dino"]:
            for var in ["Ring", "SparseRing", "Full"]:
                for metr, sort_desc in [("c125", True), ("a90", False)]: #TODO: here one can use other thresholds as well
                    addrankings.append((version+"_"+ds+var+"_"+metr, sort_desc))
        return addrankings
    def get_values(self, soup, version):
        rows = soup.contents[0].split('\n')
        get_vals = {}
        for idx, r in enumerate(rows):
            if idx == 0 or len(r) < 5:
                continue
            v = r.split(',')
            if len(v) < 5:
                raise("Error: invalid row in mview pageDump.php: " + r)
            get_vals.setdefault(v[1], {})["method"] = v[1]
            val_name = version+"_"+v[2] + "_"+ v[3]
            get_vals[v[1]][val_name.replace(" ","")] = float(v[4])
        return get_vals
        
class sorting_hd1k_flow(sorting_source_cl):
    def base_url(self):
        return "http://hci-benchmark.org/api/scores.json/?challenges=rob_flow&limit=1000000&metrics={metrics}"
    def name(self):
        return "hd1k_f"
    def formats(self):
        return {"metrics" : {"bad_pix_01", "bad_pix_03", "bad_pix_10", "maha", "mee", "runtime", "sparsity", "bad_pix_01_discont", "bad_pix_03_discont", "bad_pix_10_discont"} }
    def format_subset(self):
        return {"metrics" : {"bad_pix_01", "bad_pix_03", "bad_pix_10", "maha", "mee", "bad_pix_01_discont", "bad_pix_03_discont", "bad_pix_10_discont"} }
    def get_rows(self, soup): # no standard <tr><td> schema but uses json
        return None
    def needs_sortings(self, version):
        val_name = version + "_val"
        desc_sortings = {'hd1k_f-sparsity':True}
        return [(val_name, desc_sortings.get(version,False))]
    def get_values(self, soup, version):
        vals = json.loads(soup.text)
        val_name = version + "_val" 
        get_vals = {}
        for f in vals["results"]:
            if f["frame"] == "average_regular":
                n_entry = {self.column_id:f["algorithm"], val_name: f["value"]}
                get_vals[f["algorithm"]] = n_entry
        return get_vals
            
class sorting_middlb_stereov3(sorting_source_cl):
    def base_url(self):
        return "http://vision.middlebury.edu/stereo/eval3/table.php?dbfile=../results3/results.db&type={type}&sparsity={sparsity}&stat={stat}&mask={mask}&hasInvalid=false&ids="
    def name(self):
        return "middlb_st"
    def formats(self):
        return {"type" : {"test", "training"}, 
                "sparsity" : {"dense","sparse"}, 
                "stat" : {"bad050","bad100","bad200","bad400","avgerr","rms","A50","A90","A95","A99","time","time%2FMP","time%2FGdisp"}, 
                "mask" : {"nonocc","all"} }
    def format_subset(self):
        return {"type" : {"test"}, 
                "sparsity" : {"dense"}, 
                "stat" : {"bad050","bad100","bad200","bad400","avgerr","rms","A50","A90","A95","A99"}, 
                "mask" : {"nonocc","all"} }
    def get_rows(self, soup):
        rows = []
        all_tr = soup.find_all("tr")
        first_idx = -1
        
        for idx, one_tr in enumerate(all_tr):
            js_cont = str(one_tr)
            if js_cont.find('<tr ref="') >= 0:
                if first_idx < 0 :
                    first_idx = idx
                rows.append(one_tr)
            elif(first_idx >= 0):
                break
        return rows
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, "algName", "string"), self.TDEntry(self.rank_prefix, ["rank wtavg", "rank wtavg firstPlace"], "integer"), 
                self.TDEntry("wtavg", ["data wtavg", "data wtavg firstPlace"], "float"), self.TDEntry("date", "date", "date-us6", False)]  # TODO: add individual image results 
        
class sorting_cityscapes_semantics(sorting_source_cl):
    def base_url(self):
        return "https://www.cityscapes-dataset.com/benchmarks"
    def name(self):
        return "cityscapes_sem"
    def get_rows(self, soup):
        return soup.find("table", class_="tablepress-id-2").find_all("tr")
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("iou-class", 13, "float", False, -1), self.TDEntry("iiou-class", 14, "float", False, -1), 
                self.TDEntry("iou-category", 15, "float", False, -1), self.TDEntry("iiou-category", 16, "float", False, -1), self.TDEntry("runtime", 17, "time", False)]
    
class sorting_cityscapes_instance(sorting_source_cl):
    def base_url(self):
        return "https://www.cityscapes-dataset.com/benchmarks"
    def name(self):
        return "cityscapes_inst"
    def get_rows(self, soup):
        return soup.find("table", class_="tablepress-id-14").find_all("tr")
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("ap", 13, "float", False, -1), self.TDEntry("ap_50p", 14, "float", False, -1), 
                self.TDEntry("ap_100m", 15, "float", False, -1), self.TDEntry("ap_50m", 16, "float", False, -1), self.TDEntry("runtime", 17, "time", False)]
    
class sorting_kitti2015_stereo(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_scene_flow.php?benchmark=stereo&eval_gt={eval_gt}&eval_area={eval_area}"
    def name(self):
        return "kitti15_st"
    def formats(self):
        return {"eval_gt" : {"all", "noc"}, "eval_area" : {"all" , "est"}}
    def format_subset(self):
        return { "eval_gt" : {"all", "noc"}, "eval_area" : {"all"}}
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("d1-bg", 4, "percentage", True, 1), self.TDEntry("d1-fg", 5, "percentage", True, 1), 
                self.TDEntry("d1-all", 6, "percentage", True, 1), self.TDEntry("density", 7, "percentage", False), self.TDEntry("runtime", 8, "time", False)]

class sorting_kitti2012_stereo(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_stereo_flow.php?benchmark=stereo&table={table}&error={error}&eval={eval}"
    def name(self):
        return "kitti12_st"
    def formats(self):
        return {"table" : {"all", "refl"}, "error" : {"2", "3", "4", "5"}, "eval" : {"all" , "est"}}
    def format_subset(self):
        return None
        #return { "table" : {"all"}, "error" : {"3"}, "eval" : {"all"}}
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")
    def get_relevant_td(self, version=""):
        td_std = [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("out-noc", 4, "percentage", True, 1), self.TDEntry("out-all", 5, "percentage", True, 1), 
                  self.TDEntry("avg-nocc", 6, "float", True, 1), self.TDEntry("avg-all", 7, "float", True, 1)]
        if not version is None and version.find("refl")>0 :
            td_std.append(self.TDEntry("runtime", 8, "time", False))
        else:
            td_std.append(self.TDEntry("density", 8, "percentage", False)) 
            td_std.append(self.TDEntry("runtime", 9, "time", False))
        return td_std
    
class sorting_kitti2015_flow(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_scene_flow.php?benchmark=flow&eval_gt={eval_gt}&eval_area={eval_area}"
    def name(self):
        return "kitti15_f"
    def formats(self):
        return {"eval_gt" : {"all", "noc"}, "eval_area" : {"all" , "est"}}
    def format_subset(self):
        return {"eval_gt" : {"all", "noc"}, "eval_area" : {"all"}}
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")[1:]
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("fl-bg", 4, "percentage", True, 1), self.TDEntry("fl-fg", 5, "percentage", True, 1), 
                self.TDEntry("fl-all", 6, "percentage", True, 1), self.TDEntry("density", 7, "percentage", False), self.TDEntry("runtime", 8, "time", False)]

class sorting_kitti2012_flow(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_stereo_flow.php?benchmark=flow&error={error}&eval={eval}"
    def name(self):
        return "kitti12_f"
    def formats(self):
        return {"error" : {"2", "3", "4", "5"}, "eval" : {"all" , "est"}}
    def format_subset(self):
        return {"error" : {"3"}, "eval" : {"all"}}
        #return None
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")[1:]
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("out-noc", 4, "percentage", True, 1), self.TDEntry("out-all", 5, "percentage", True, 1), 
                self.TDEntry("avg-noc", 6, "float", True, 1), self.TDEntry("avg-all", 7, "float", True, 1), self.TDEntry("density", 8, "percentage", False), self.TDEntry("runtime", 9, "time", False)]


class sorting_kitti_depth(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_depth.php?benchmark=depth_prediction"
    def name(self):
        return "kitti_d"
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("sl-log", 4, "float", False,1), self.TDEntry("sq-error-rel", 5, "float", False, 1), 
                self.TDEntry("abs-error-rel", 6, "float", False, 1), self.TDEntry("irmse", 7, "float", False, 1), self.TDEntry("runtime", 8, "time", False)]

class sorting_kitti_semantics(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_semseg.php?benchmark=semantics2015"
    def name(self):
        return "kitti_sem"
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("iou-class", 4, "float", False,1), self.TDEntry("iiou-class", 5, "float", False,1), self.TDEntry("iou-category", 6, "float", False,1), self.TDEntry("iiou-category", 7, "float", False,1), self.TDEntry("runtime", 8, "time", False)]

class sorting_kitti_instance(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_instance_seg.php?benchmark=instanceSeg2015"
    def name(self):
        return "kitti_i"
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("ap", 4, "float", False,1), self.TDEntry("ap50", 5, "float", False,1), self.TDEntry("runtime", 6, "time", False)]
    
class sorting_eth3d_stereo(sorting_source_cl):
    def base_url(self):
        return "https://www.eth3d.net/low_res_two_view?coverage={coverage}&set={set}&metric={metric}&mask={mask}"
    def name(self):
        return "eth3d_st"
    def formats(self):
        return {"coverage" : {"0", "1", "2", "3", "4"}, "set" : {"test", "training"},
                                          "metric" : {"bad-0-5", "bad-1-0", "bad-2-0", "bad-4-0", "avgerr", "rms", "a50", "a90", "a95", "a99", "time", "coverage"},
                                          "mask" : {"all", "non_occ"}}
    def format_subset(self):
        return {"coverage" : {"0"}, "set" : {"test"},
                        "metric" : {"bad-0-5", "bad-1-0", "bad-2-0", "bad-4-0", "avgerr", "rms", "a50", "a90", "a95", "a99"},
                        "mask" : {"all", "non_occ"}}
    def get_rows(self, soup):
        all_tr = soup.find("table", class_="table-condensed").find_all("tr")
        rows = []
        for one_tr in all_tr:
            js_cont = str(one_tr)
            if js_cont.find('<a href="/result') >= 0:
                rows.append(one_tr)
        return rows
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry(self.rank_prefix, 2.5, "string", True), self.TDEntry("val", 2, "float", True)]

class sorting_eth3d_mvs(sorting_source_cl):
    def base_url(self):
        return "https://www.eth3d.net/high_res_multi_view?set={set}&metric={metric}&tolerance_id={tolerance_id}"
    def name(self):
        return "eth3d_mvs"
    def formats(self):
        return {"set" : {"test", "training"},
                "metric" : {"f1-score", "accuracy", "completeness", "time"},
                "tolerance_id" : {"1", "2", "3", "4", "5", "6"}}
    def format_subset(self):
        return {"set" : {"test"},
                "metric" : {"f1-score", "accuracy", "completeness"},
                "tolerance_id" : {"1", "2", "3", "4", "5", "6"}}
    def get_rows(self, soup):
        all_tr = soup.find("table", class_="table-condensed").find_all("tr")
        rows = []
        for one_tr in all_tr:
            js_cont = str(one_tr)
            if js_cont.find('<a href="/result') >= 0:
                rows.append(one_tr)
        return rows
    def get_relevant_td(self, version=""):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("val", 2, "float", True)]

def get_all_sources_rob18():
    all_stereo_sources = [sorting_eth3d_stereo(), sorting_middlb_stereov3(), sorting_kitti2012_stereo(), sorting_kitti2015_stereo()]
    all_flow_sources = [sorting_middlb_flow(), sorting_kitti2015_flow(), sorting_kitti2012_flow(), sorting_sintel_flow(), sorting_hd1k_flow() ]  # missing: hd1k
    all_mvs_sources = [sorting_middlb_mvs(), sorting_eth3d_mvs()]
    all_depth_sources = [sorting_kitti_depth()]  # TODO: scannet
    all_semantic_sources = [sorting_cityscapes_semantics(), sorting_kitti_semantics()]  # TODO: Scannet, Wilddash
    all_instance_sources = [sorting_cityscapes_instance(), sorting_kitti_instance()]  # TODO: Scannet, Wilddash
    all_sources = [("stereo", all_stereo_sources), ("flow", all_flow_sources), ("mvs", all_mvs_sources),
                   ("depth", all_depth_sources), ("semantic", all_semantic_sources), ("instance", all_instance_sources)]
    return all_sources
    
