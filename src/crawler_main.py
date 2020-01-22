#!/usr/bin/env python
import shutil, os, logging, sys
from multi_ranking import get_joined_ranking
import datacrawler as dc

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    #Parameters changing way that multi-ranking is applied
    read_only = 1 #0: fetch online data, calc csv and multi-ranks, 1: only calc csv and multi-ranks, 2: only calc multi-ranks
    batchsize_calc = 10 # speed vs. accuracy of multijoiner; values over 10 are not recommended (very long execution times); -1 turns iterative process off -> can take forever
    max_sort_calc = 10 #20 # sprevents long calcuations for multijoiner; after sorting max_sort_calc entries, the remaining entries are quickly sorted by their mean ranks
    only_subset = True # calculate joined ranking over defined subsets instead of all available rankings
    allow_fuzzy_namecmp = 0 # 0: compare names exact, 1: ignore case, 2: ignore any non-alphanumeric character
    
    name_joined_rank = "joined_"+dc.rank_prefix
    all_sources = dc.get_all_sources_rob18()
    #all_sources = [("flow", [dc.sorting_sintel_flow(), dc.sorting_hd1k_flow()])]
    #all_sources = [("object_detection", [dc.sorting_oid_obj()])]
    #all_sources = [("stereo", [dc.sorting_middlb_stereov3(), dc.sorting_kitti2015_stereo(), dc.sorting_eth3d_stereo()])]
    
    #all_sources = [("semantic", [dc.sorting_cityscapes_semantics(), dc.sorting_kitti_semantics(), dc.sorting_scannet_semantics(), dc.sorting_wilddash_semantics()])]
    white_list = None
    
    if len(sys.argv) > 1:
        #read whitelist txt file
        with open(sys.argv[1]) as wlfile:
            wl_lines = wlfile.readlines()
        white_list = [dc.res_name_fuzzy_cmp(l.split(';')[1].strip(), allow_fuzzy_namecmp) for l in wl_lines]
    
    res_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),"../results")
    tmp_dir_root = os.path.join(os.path.dirname(os.path.realpath(__file__)),"../data")
    
    if read_only == 0:
        tmp_dir = os.path.join(tmp_dir_root, str(int(dc.unix_time_now()))+"_tmp")
        archive_dir = os.path.join(tmp_dir_root,"archive")
    else:
        #this is for debugging only: unzip one of the archived tmp folders into data/_tmp/
        tmp_dir = os.path.join(tmp_dir_root, "_tmp")
        archive_dir = None
    
    for d in [res_dir,tmp_dir,archive_dir]:
        if not d is None and not os.path.exists(d):
            os.makedirs(d)
    
    rclogger = dc.setup_logging(tmp_dir+'/robcrawler.log')
    rclogger.info("Started crawler, tmp_dir: %s, res_dir: %s, archive_dir: %s ", tmp_dir, res_dir, archive_dir)
    
    #first download all resources
    sucess_subsets = None
    if read_only <= 0:
        sucess_subsets = dc.fetch_datasets(all_sources, tmp_dir, only_subset=False) #always crawl all online resources (this allows to revisit historic data to the full extent)
    
    #per challenge...
    result_files = []
    for name, sources in all_sources:
        #skip calculation of this subset as the respective online resource could not be crawled       
        if not sucess_subsets is None and not name in sucess_subsets:
            rclogger.warning("Skipping calculations for subset "+name+" as not all necessary online resources could be crawled.")
            continue 
        try:
            all_vals = dc.get_joined_dataset(sources, tmp_dir, only_subset=only_subset, read_only=read_only, allow_fuzzy_namecmp = allow_fuzzy_namecmp)
            if len(all_vals) <= 0:
                continue # skip this subset, it has no data (on purpose; otherwise get_joined_dataset would throw an exception)
            #one large csv containing all the data
            all_keys, all_rankings = dc.get_all_rankings(all_vals)
            header_list = [dc.column_id] + sorted(list(all_rankings))
            header_list += sorted(list(set(all_keys) - set(header_list)))
            dc.save_as_csv(header_list, all_vals.keys(), all_vals, tmp_dir + "/all_%s.csv" % name, order_name="idx")
            filtered_vals = dc.remove_incomplete(all_vals, all_rankings, white_list)
            
            #gather only the intersection between all datasets
            dc.save_as_csv(header_list, filtered_vals.keys(), filtered_vals, tmp_dir + "/filtered_%s.csv" % name, order_name="idx")
            if len(filtered_vals.keys()) <= 0 :
                rclogger.warning("Datasets of subset "+name+ " do not share common methods; skipping rank joining..")
                continue
            
            all_sources_calculated = True
            subset_result_files = []  #result files per source; these are only appended to result_files if all calculations of all associated datasources worked
            subset_ranking_names = []
            #create subset joined rankings per benchmark (this allows easier viewing on the website)
            for s in sources:
                if s.format_subset() is None: # this source is only archived for historical data reasons and is not used for acquiring rankings
                    continue
                src_name = s.name()
                rclogger.info("Calculating joined ranking for dataset "+src_name+ " of subset "+name)
                src_keys, src_rankings = dc.get_all_rankings(filtered_vals, src_name)
                src_filtered_vals = dc.remove_incomplete(filtered_vals, src_rankings, None)
                header_list_subset = [dc.column_id] + dc.mixed_ranking_vals_headers(src_keys, sorted(src_rankings), rank_first=True) #interleave rank / val
                subjoined_ranking = get_joined_ranking(list(src_filtered_vals.values()), dc.column_id, src_rankings, batchsize = batchsize_calc, max_calc= max_sort_calc, weight_p_ranking={}, rclogger = rclogger)
                
                if len(subjoined_ranking) <= 0:
                    rclogger.warning("Skipping calculations for subset "+name+", could not calculate joined ranking for dataset "+src_name)
                    all_sources_calculated = False
                    break 

                sub_join_file = tmp_dir + "/subjoined_%s.csv" % src_name
                sub_join_name = name_joined_rank+"_"+src_name
                #add new subset ranking to filtered_vals
                filtered_vals = dc.add_new_ranking(filtered_vals, dc.column_id, sub_join_name, subjoined_ranking)
                display_vals = dc.normalize_rankings(filtered_vals, src_rankings, add_old_rank = True)
                dc.save_as_csv(header_list_subset, subjoined_ranking, display_vals, sub_join_file, order_name=sub_join_name)
                
                subset_ranking_names.append(sub_join_name)
                subset_result_files.append(sub_join_file)
                
            if not all_sources_calculated:
                continue
                      
            rclogger.info("Calculating joined ranking for subset "+name)
            joined_ranking = get_joined_ranking(filtered_vals, dc.column_id, subset_ranking_names, batchsize = batchsize_calc, max_calc= max_sort_calc, weight_p_ranking = {}, rclogger = rclogger)
            # Balance the influence of each dataset (sum of wheights per dataset are the same)
            # wheights = dc.calc_weight_per_benchmark(all_rankings, sources)
            # joined_ranking = get_joined_ranking(list(filtered_vals.values()), dc.column_id, all_rankings,  batchsize = batchsize_calc, max_calc= max_sort_calc, weight_p_ranking = wheights, rclogger = rclogger)
                        
            if len(joined_ranking) <= 0:
                rclogger.warning("Skipping calculations for subset "+name+" as no overall joined ranking could be calculated.")
                continue 
            
            filtered_vals = dc.add_new_ranking(filtered_vals, dc.column_id, name_joined_rank, joined_ranking)
            overall_res_file = tmp_dir + "/ranked_full_%s.csv" % name
            all_val = [r.replace(dc.rank_prefix+"_","") for r in all_rankings]
            dc.save_as_csv([name_joined_rank, dc.column_id]+subset_ranking_names+all_rankings+all_val, joined_ranking, filtered_vals, overall_res_file)

            #this file is a condensed version having the overall joined ranks and the individual joined ranks per dataset
            condensed_res_file = tmp_dir + "/ranked_condensed_%s.csv" % name
            dc.save_as_csv([name_joined_rank, dc.column_id]+subset_ranking_names, joined_ranking, filtered_vals, condensed_res_file)
            
            
            #each subset creates these result files: one condensed joined rank of all participating datasets and one detained joined rank csv per dataset (for this challenge)   
            result_files.append(condensed_res_file)
            result_files += subset_result_files
            
        except  Exception as e:
            rclogger.error("Skipping calculations for subset "+name+"; Exception: "+str(e))
            continue 
        

    
    for r in result_files:
        shutil.copy2(r,res_dir)
    
    if not archive_dir is None:
        #archive all artifacts
        try:
            tmp_name = tmp_dir.replace('\\','/').split('/')[-1]
            archive_name = os.path.join(archive_dir,tmp_name)
            archive_ext = "zip"
            shutil.make_archive(archive_name, archive_ext, root_dir=tmp_dir, base_dir=tmp_dir)
            #release file handle (log file resides in tmp dir!) 
            for handler in rclogger.handlers[:]:
                handler.close()
                rclogger.removeHandler(handler)
            #clean tmp folder
            if os.path.exists(tmp_dir) and os.path.isdir(tmp_dir) and os.path.exists(archive_name+"."+archive_ext):
                shutil.rmtree(tmp_dir)
        except Exception as e:
            print("Error: Clean-up tmp dir "+ tmp_dir + " or archive to file " + archive_name + "."+archive_ext+" failed; Exception: "+ str(e))
    
