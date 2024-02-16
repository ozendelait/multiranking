#Hint: escape forward slashes (char '/') using %2F; otherwise the automatic naming for html/csv files will fail
from .sorting_source import sorting_source_cl, sorting_source_json
import sys
import json
from string import digits as strdg
from . import csv_tools as ct

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
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("EPEall", 1, "float", True, 1),
                self.TDEntry("EPEmatched", 2, "float", True, 1),
                self.TDEntry("EPEunmatched", 3, "float", True, 1), self.TDEntry("d0-10", 4, "float", True, 1),
                self.TDEntry("d10-60", 5, "float", True, 1),
                self.TDEntry("d60-140", 6, "float", True, 1), self.TDEntry("s0-10", 7, "float", True, 1),
                self.TDEntry("s10-40", 8, "float", True, 1), self.TDEntry("s40p", 9, "float", True, 1)]

class sorting_sintel_depth(sorting_source_cl):
    def base_url(self):
        return "https://sintel-depth.csail.mit.edu/leaderboard"
    def name(self):
        return "sintel_d"
    def get_rows(self, soup):
        all_tr = soup.find_all("table")[0].find_all("tr")[1:]
        return all_tr
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("SILog_final", 1, "float", True, 1),
                self.TDEntry("SqErrorRel_final", 2, "float", True, 1),
                self.TDEntry("AbsErrorRel_final", 3, "float", True, 1),
                self.TDEntry("iRMSE_final", 4, "float", True, 1), self.TDEntry("SILog_clean", 5, "float", True, 1),
                self.TDEntry("SqErrorRel_clean", 6, "float", True, 1),
                self.TDEntry("AbsErrorRel_clean", 7, "float", True, 1),
                self.TDEntry("iRMSE_clean", 8, "float", True, 1)]

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
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("avrg_rnk", 1, "float", True, 1)]  # TODO: add scores from images
        
class sorting_source_codacsv(sorting_source_cl):
    def get_rows(self, soup):
        return None  # no standard <tr><td> schema but uses csv
    def needs_sortings(self, version): #works for obj and inst
        return [(version + "_" + metr, True) for metr in ["ap","ap-50","ap-75"]]
    def get_values(self, soup, version, line=-1):
        if len(soup.contents) == 0:
            return {}
        rows = soup.contents[0]
        if not isinstance(rows,str) or (sys.version[0] == 2 and not isinstance(rows, basestring)):
            return {}
        rows = rows.split('\n')
        get_vals = {}
        sortings = self.needs_sortings(version)
        column_idx = 1  # this is often the user instead of the method / team name...
        for idx, r in enumerate(rows):
            if idx == 0:
                for col_check in ["Method", "Team Name"]:
                    if col_check in r:
                        column_idx = [idx for idx, v in enumerate(r.split(',')) if col_check in v][0]
                        break
                continue
            if len(r) < 5:
                continue
            if max(r.find('animal--bird'), r.find('PQ_Bird'), r.find('No data available')) >= 0: #reached detail view
                break
            v = r.split(',')
            if len(v) < len(sortings)+2:
                raise "Error: invalid row for "+ self.name() +": " + r
            get_vals.setdefault(v[column_idx], {})[self.column_id] = v[column_idx-1] if column_idx > 0 and "_RVC" in v[column_idx-1] else v[column_idx] #hack to use team name instead of method if it matches the _RVC postfix
            for idx0, (val_name,_) in enumerate(sortings):
                val_corr = v[column_idx+1+idx0].split('(')
                get_vals[v[column_idx]][val_name] = float(val_corr[0])
                if len(val_corr) > 1:
                    get_vals[v[column_idx]][self.rank_prefix+"_"+val_name] = int(val_corr[1].split(')')[0])

        return get_vals

class sorting_mvd_objdet(sorting_source_codacsv):
    def base_url(self): 
        return "https://codalab.lisn.upsaclay.fr/competitions/7515/results/12355/data"
    def name(self):
        return "mvd_obj"

class sorting_mvd_semantics(sorting_source_codacsv):
    def base_url(self):
        return "https://codalab.lisn.upsaclay.fr/competitions/5821/results/8772/data"
    def name(self):
        return "mvd_sem"
    def needs_sortings(self, version):
        return [(version + "_" + metr, True) for metr in ["iou"]]
 
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
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, "algName", "string"), self.TDEntry(self.rank_prefix, ["rank wtavg", "rank wtavg firstPlace"], "integer"), 
                self.TDEntry("wtavg", ["data wtavg", "data wtavg firstPlace"], "float"), self.TDEntry("date", "date", "date-us6", False)]  # TODO: add individual image results 
            
class sorting_scannet_semantics(sorting_source_cl):
    def base_url(self):
        return "http://kaldir.vc.in.tum.de/scannet_benchmark/semantic_label_2d"
    def name(self):
        return "scannet_sem"
    def get_rows(self, soup):
        return soup.find_all("table", class_="table-condensed")[0].find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("avg-iou", 2, "float", False, -1)]
    
class sorting_scannet_depth(sorting_source_cl):
    def base_url(self):
        return "http://dovahkiin.stanford.edu/adai"
    def name(self):
        return "scannet_d"
    def get_rows(self, soup):
        return soup.find_all("table", class_="table-condensed")[0].find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("abs-rel", 2, "float", False, 1), self.TDEntry("inv-mae", 3, "float", False, 1), 
                self.TDEntry("inv-rmse", 4, "float", False, 1),  self.TDEntry("scale-invar", 9, "float", False, 1), self.TDEntry("sqr-rel", 10, "float", False, 1)]

class sorting_cityscapes_semantics(sorting_source_cl):
    def base_url(self):
        return "https://www.cityscapes-dataset.com/benchmarks"
    def name(self):
        return "cityscapes_sem"
    def get_rows(self, soup):
        return soup.find("table", class_="tablepress-id-2").find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("iou-class", 13, "float", False, -1), self.TDEntry("iiou-class", 14, "float", False, -1), 
                self.TDEntry("iou-category", 15, "float", False, -1), self.TDEntry("iiou-category", 16, "float", False, -1), self.TDEntry("runtime", 17, "time", False)]
   
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
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("d1-bg", 4, "percentage", True, 1), self.TDEntry("d1-fg", 5, "percentage", True, 1), 
                self.TDEntry("d1-all", 6, "percentage", True, 1), self.TDEntry("density", 7, "percentage", True), self.TDEntry("runtime", 8, "time", False)]

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
    def get_relevant_td(self, version="", line=-1):
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
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("fl-bg", 4, "percentage", True, 1), self.TDEntry("fl-fg", 5, "percentage", True, 1), 
                self.TDEntry("fl-all", 6, "percentage", True, 1), self.TDEntry("density", 7, "percentage", True), self.TDEntry("runtime", 8, "time", False)]

class sorting_kitti2012_flow(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_stereo_flow.php?benchmark=flow&error={error}&eval={eval}"
    def name(self):
        return "kitti12_f"
    def formats(self):
        return {"error" : {"2", "3", "4", "5"}, "eval" : {"all" , "est"}}
    def format_subset(self):
        return None
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")[1:]
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("out-noc", 4, "percentage", True, 1), self.TDEntry("out-all", 5, "percentage", True, 1), 
                self.TDEntry("avg-noc", 6, "float", True, 1), self.TDEntry("avg-all", 7, "float", True, 1), self.TDEntry("density", 8, "percentage", True), self.TDEntry("runtime", 9, "time", False)]

class sorting_kitti_depth(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_depth.php?benchmark=depth_prediction"
    def name(self):
        return "kitti_d"
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("si-log", 4, "float", False,1), self.TDEntry("sq-error-rel", 5, "float", False, 1),
                self.TDEntry("abs-error-rel", 6, "float", False, 1), self.TDEntry("irmse", 7, "float", False, 1), self.TDEntry("runtime", 8, "time", False)]

class sorting_kitti_instance(sorting_source_cl):
    def base_url(self):
        return "http://www.cvlibs.net/datasets/kitti/eval_instance_seg.php?benchmark=instanceSeg2015"
    def name(self):
        return "kitti_i"
    def get_rows(self, soup):
        return soup.find("table", class_="results").find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"), self.TDEntry("ap", 4, "float", False,-1), self.TDEntry("ap50", 5, "float", False,-1), self.TDEntry("runtime", 6, "time", False)]
                
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
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry(self.rank_prefix, 2.5, "string", True), self.TDEntry("val", 2, "float", True)]

class sorting_eth3d_high_mvs(sorting_source_cl):
    def base_url(self):
        return "https://www.eth3d.net/high_res_multi_view?set={set}&metric={metric}&tolerance_id={tolerance_id}"
    def name(self):
        return "eth3d_high_mvs"
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
    def get_relevant_td(self, version="", line=-1):
        type0 = "percentage"
        sort_order = -1
        if version.find("time") >= 0:
            type0 = "float"
            sort_order = 1
            
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("high-res-mv", 3, type0, True,sort_order), self.TDEntry("indoor", 4, type0, True,sort_order), self.TDEntry("outdoor", 5, type0, True,sort_order)]
    
class sorting_eth3d_low_mvs(sorting_source_cl):
    def base_url(self):
        return "https://www.eth3d.net/low_res_many_view?set={set}&metric={metric}&tolerance_id={tolerance_id}"
    def name(self):
        return "eth3d_low_mvs"
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
    def get_relevant_td(self, version="", line=-1):
        type0 = "percentage"
        sort_order = -1
        if version.find("time") >= 0:
            type0 = "float"
            sort_order = 1
            
        return [self.TDEntry(self.column_id, 0, "string"), self.TDEntry("low-res-mv", 3, type0, True,sort_order), self.TDEntry("indoor", 4, type0, True,sort_order), self.TDEntry("outdoor", 5, type0, True,sort_order)]
          

class sorting_wilddash_prototype(sorting_source_cl):
    eval_framesets = ['classic', 'negative', 'blur_high', 'blur_none', 'blur_low',
                      'coverage_high', 'coverage_none', 'coverage_low',
                      'distortion_high', 'distortion_none', 'distortion_low',
                      'hood_high', 'hood_none', 'hood_low',
                      'occlusion_high', 'occlusion_none', 'occlusion_low',
                      'overexp_high', 'overexp_none', 'overexp_low',
                      'particles_high', 'particles_none', 'particles_low',
                      'screen_high', 'screen_none', 'screen_low',
                      'underexp_high', 'underexp_none', 'underexp_low',
                      'variations_high', 'variations_none', 'variations_low']
    eval_framesets = ['average']
    algo_disp_name = "algorithm_display_name"
    access_elem = "label_class" # "frame_set"
    expect_suffix = None 
    #algo_disp_name = "algorithm_display_name"
    def base_url(self):
        return None
    def get_rows(self, soup): # no standard <tr><td> schema but uses json
        return None
    def needs_sortings(self, version):
        all_sortings = []
        desc_sortings = {}
        for f in self.eval_framesets:
            val_name = version +"_"+ f
            all_sortings.append((val_name, desc_sortings.get(version,True)))
        return all_sortings
    def get_values(self, soup, version, line=-1):
        vals = json.loads(soup.text)
        get_vals = {}
        transl_rem_digits = {ord(d):'' for d in strdg}
        for f in vals["results"]:
            if not self.access_elem in f:
                continue
            fr_name = f[self.access_elem]
            fr_name = str(fr_name).translate(transl_rem_digits) #remove unnecessary postfixes
            if fr_name in self.eval_framesets and not f["value"] is None:
                val_name = version +"_"+ fr_name
                if not f[self.algo_disp_name] in get_vals:
                    n_entry = {self.column_id:f[self.algo_disp_name], val_name: f["value"]}
                    get_vals[f[self.algo_disp_name]] = n_entry
                else:
                    get_vals[f[self.algo_disp_name]][val_name] = f["value"]
        #for algo, vals in get_vals.items():
        #    for e in self.eval_framesets:
        #        check_v = version +"_"+ e
        #        if not check_v in vals:
        #            print("Warning: did not get values for id "+e+" for algorithm "+algo)
        return get_vals

class sorting_wilddash2_semantics(sorting_wilddash_prototype):
    expect_suffix = "4"
    def base_url(self):
        return "https://wilddash.cc/api/scores.json/?challenges=semantic_rob_2020&limit=1000000&frame_sets=summary_average"+self.expect_suffix+"&metrics={metrics}"
    def name(self):
        return "wilddash2_sem"
    def formats(self):
        return {"metrics" : {"iou_class2", "iou_category2", "iiou_class2", "iiou_category2"}}
    def format_subset(self):
        return {"metrics" : {"iou_category2","iou_class2", "iou_category2", "iiou_class2", "iiou_category2"}}

class sorting_objects365_objdet(sorting_source_cl):
    def base_url(self):
        return "https://www.objects365.org/{track}_track.html"
    def name(self):
        return "objects365_obj"
    def formats(self):
        return {"track" : {"tiny", "full"}}
    def format_subset(self):
        return {"track" : {"full"}}
    def get_rows(self, soup):
        return soup.find("div", class_="container").find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        start_col = 0 if line < 4 else 1 #the first three entries have another markup
        return [self.TDEntry(self.column_id, start_col, "string"), self.TDEntry("institute", start_col+1, "string"), self.TDEntry("ap", start_col+2, "float", False, -1)]

class sorting_ade20k_semantics(sorting_source_cl):
    def base_url(self):
        return "http://sceneparsing.csail.mit.edu/eval/leaderboard_iframe.php"
    def name(self):
        return "ade20k_sem"
    def get_rows(self, soup):
        return soup.find("table", {"id": "report"}).find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"),self.TDEntry("affiliation", 1, "string"),
                self.TDEntry("acc", 2, "float", False, -1), self.TDEntry("iou", 3, "float", False, -1), 
                self.TDEntry("score", 4, "float", False,-1), self.TDEntry("submtime", 5, "time", False)]

class sorting_viper_depth(sorting_source_cl):
    def base_url(self):
        return "https://playing-for-benchmarks.org/leaderboards/monodepth/"
    def name(self):
        return "viper_d"
    def get_rows(self, soup):
        return soup.find_all("table", class_="table-hover")[0].find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"),
                self.TDEntry("si-log", 2, "float", False, 1),
                self.TDEntry("abs-error-rel", 3, "float", False, 1),
                self.TDEntry("irmse", 4, "float", False, 1),
                self.TDEntry("runtime", 5, "time", False)]

class sorting_viper_semantics(sorting_source_cl):
    def base_url(self):
        return "https://playing-for-benchmarks.org/leaderboards/seg_cls_img/"
    def name(self):
        return "viper_sem"
    def get_rows(self, soup):
        return soup.find_all("table", class_="table-hover")[0].find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"), 
                self.TDEntry("iou-all", 2, "float", False, -1),
                self.TDEntry("iou-day", 3, "float", False, -1),
                self.TDEntry("iou-sunset", 4, "float", False, -1),
                self.TDEntry("iou-rain", 5, "float", False, -1),
                self.TDEntry("iou-snow", 6, "float", False, -1),
                self.TDEntry("iou-night", 7, "float", False, -1),
                self.TDEntry("runtime", 8, "time", False)]

class sorting_viper_flow(sorting_source_cl):
    def base_url(self):
        return "https://playing-for-benchmarks.org/leaderboards/flow_2d/"
    def name(self):
        return "viper_f"
    def get_rows(self, soup):
        return soup.find_all("table", class_="table-hover")[0].find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"),
                self.TDEntry("wauc-all", 2, "float", False, -1),
                self.TDEntry("wauc-day", 3, "float", False, -1),
                self.TDEntry("wauc-sunset", 4, "float", False, -1),
                self.TDEntry("wauc-rain", 5, "float", False, -1),
                self.TDEntry("wauc-snow", 6, "float", False, -1),
                self.TDEntry("wauc-night", 7, "float", False, -1),
                self.TDEntry("runtime", 8, "time", False)]

class sorting_viper_panoptic(sorting_source_cl):
    def base_url(self):
        return "https://playing-for-benchmarks.org/leaderboards/seg_pano_img/"
    def name(self):
        return "viper_pano"
    def get_rows(self, soup):
        return soup.find_all("table", class_="table-hover")[0].find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"),
                self.TDEntry("pq-mean", 2, "float", False, -1),
                self.TDEntry("sq-mean", 3, "float", False, -1),
                self.TDEntry("rq-mean", 4, "float", False, -1),
                self.TDEntry("runtime", 5, "time", False)]

class sorting_rabbitai_depth(sorting_source_cl):
    def base_url(self):
        return "https://rabbitai.de/benchmark"
    def name(self):
        return "rabbitai_d"
    def get_rows(self, soup):
        return soup.find_all("table")[0].find_all("tr")
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 0, "string"),
                self.TDEntry("avg30", 1, "float", False, 1),
                self.TDEntry("miss30", 2, "float", False, 1),
                self.TDEntry("fake30", 3, "float", False, 1),
                self.TDEntry("miss-st30", 4, "float", False, 1),
                self.TDEntry("fake-st30", 5, "float", False, 1),
                self.TDEntry("bump30", 6, "float", False, 1),
                self.TDEntry("avg-scale-err", 7, "float", False, 1),
                self.TDEntry("avg-offset", 8, "float", False, 1),
                self.TDEntry("si-log", 9, "percentage", False, 1),
                self.TDEntry("sq-rel", 10, "percentage", False, 1),
                self.TDEntry("abs-rel", 11, "percentage", False, 1),
                self.TDEntry("inv-rmse", 12, "float", False, 1)]

class sorting_coco_objdet(sorting_source_codacsv):
    def base_url(self):
        return "https://codalab.lisn.upsaclay.fr/competitions/6420/results/9498/data"
    def name(self):
        return "coco_obj"
    def needs_sortings(self, version):
        return [(version + "_" + metr, True) for metr in ["ap","ap-50","ap-75","ap-l","ap-m","ap-s","ar-l","ar-m1","ar-max10","ar-max100","ar-m","ar-s"]]

class sorting_coco_semantics(sorting_source_codacsv):
    def base_url(self):
        return None
    def name(self):
        return "coco_sem"
    def needs_sortings(self, version):
        return [(version + "_" + metr.lower().replace(" ","-"), True) for metr in ["PQ", "SQ", "RQ", "PQ Things", "SQ Things", "RQ Things", "PQ Stuff", "SQ Stuff", "RQ Stuff"]]

class sorting_kaggle_template(sorting_source_cl):
    algo_disp_name = "teamName"
    keep_keys = ['score']
    def base_url(self):
        return None
    def get_rows(self, soup): # no standard <tr><td> schema but uses json
        return None
    def needs_sortings(self, version):
        return [(version+'_score', True)]
    def get_values(self, soup, version, line=-1):
        vals = json.loads(soup.text)
        get_vals = {}
        for f in vals["submissions"]:
            if not self.algo_disp_name in f.keys():
                continue
            id0 = f[self.algo_disp_name]
            get_vals[id0] = {self.column_id:id0}
            for key, val in f.items():
                if key in self.keep_keys:
                    get_vals[id0][version+"_"+key] = float(val)
        return get_vals
        
class sorting_oid_objdet(sorting_kaggle_template):
    def base_url(self):
        return "kaggle://open-images-object-detection-rvc-2022-edition"
    def name(self):
        return "oid_obj"         

class sorting_coda_semantics(sorting_source_cl):
    def base_url(self):
        return "https://codalab.lisn.upsaclay.fr/competitions/5821#results"
    def name(self):
        return "coda_sem"
    def get_rows(self, soup):
        return soup.find("table", class_="table").find_all("tr")[1:]
    def get_relevant_td(self, version="", line=-1):
        return [self.TDEntry(self.column_id, 1, "string"), 
                            self.TDEntry("score", 2, "float", False,-1),self.TDEntry("oldpos", 0, "int", False)]


def get_all_sources_rvc2022():
    all_stereo_sources = [sorting_eth3d_stereo(), sorting_middlb_stereov3(),  sorting_kitti2015_stereo()]
    all_flow_sources = [sorting_middlb_flow(), sorting_kitti2015_flow(), sorting_sintel_flow(), sorting_viper_flow() ]
    all_depth_sources = [sorting_kitti_depth(), sorting_viper_depth(), sorting_sintel_depth()]
    all_objdet_sources = [sorting_oid_objdet(), sorting_coco_objdet(), sorting_mvd_objdet()]
    all_semantic_sources = [sorting_ade20k_semantics(),  sorting_cityscapes_semantics(), sorting_mvd_semantics(), sorting_scannet_semantics(), sorting_viper_semantics(), sorting_wilddash2_semantics()]
    codalab_check = [sorting_coda_semantics()]
    all_sources = [#("stereo", all_stereo_sources), ("flow", all_flow_sources), ("semantic", all_semantic_sources)
                   # only scale-invariant metrics allowed -> check again
                   #("depth", all_depth_sources),
                   # some obj. det. leaderboards need fixing/visibility changes 
                   #("objdet", all_objdet_sources)
                   ("coda", codalab_check)
                   ]
    return all_sources
    
