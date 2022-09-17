#!/usr/bin/env python
import shutil, os, logging, sys
from multi_ranking import get_joined_ranking
import datacrawler as dc

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    #Parameters changing way that multi-ranking is applied
    read_only = 0 #0: fetch online data, calc csv and multi-ranks, 1: only calc csv and multi-ranks, 2: only calc multi-ranks
    batchsize_calc = 10 # speed vs. accuracy of multijoiner; values over 10 are not recommended (very long execution times); -1 turns iterative process off -> can take forever
    max_sort_calc = 10 #20 # sprevents long calcuations for multijoiner; after sorting max_sort_calc entries, the remaining entries are quickly sorted by their mean ranks
    only_subset = True # calculate joined ranking over defined subsets instead of all available rankings
    allow_fuzzy_namecmp = 1 # 0: compare names exact, 1: ignore case, 2: ignore any non-alphanumeric character
    fake_incomplete_rank = 4096 # rank used in csv for incomplete submissions

    name_joined_rank = "joined_"+dc.rank_prefix
    all_sources = dc.get_all_sources_rvc2020()
    #all_sources = [("flow", [dc.sorting_middlb_flow(), dc.sorting_kitti2015_flow(), dc.sorting_sintel_flow(), dc.sorting_viper_flow()])]
    #all_sources += [("stereo", [dc.sorting_eth3d_stereo(), dc.sorting_middlb_stereov3(), dc.sorting_kitti2015_stereo()])]
    #all_sources += [("depth", [dc.sorting_kitti_depth(), dc.sorting_rabbitai_depth(), dc.sorting_viper_depth(), dc.sorting_sintel_depth()])]
    #all_sources += [("objdet", [dc.sorting_oid_objdet(), dc.sorting_coco_objdet(), dc.sorting_mvd_objdet()])]
    #all_sources = [("i", [dc.sorting_viper_panoptic()])]
    #all_sources = [s for s in dc.get_all_sources_rvc2020() if s[0]=="panoptic"]
    white_list = None
    renaming_methods = {}
    if len(sys.argv) > 1:
        #read whitelist txt file
        with open(sys.argv[1]) as wlfile:
            wl_lines = wlfile.readlines()
        white_list = [dc.res_name_fuzzy_cmp(l.split(';')[1].strip(), allow_fuzzy_namecmp) for l in wl_lines]
    if len(sys.argv) > 2:
        #read renaming list to correctly map ill-named submissions; format: lines with source.name();old_name;new_name   
        with open(sys.argv[2]) as rnfile:
            rn_lines = rnfile.readlines()
        for l in rn_lines:
            one_renaming = [lsep.strip() for lsep in l.split(';')]
            #skip invalid/commented lines
            if len(one_renaming) != 3 or len(one_renaming[0]) == 0 or one_renaming[0][0] == "#":
                continue
            renaming_methods.setdefault(one_renaming[0],{})[one_renaming[1]] = one_renaming[2]

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
    success_subsets = None
    if read_only <= 0:
        success_subsets = dc.fetch_datasets(all_sources, tmp_dir, only_subset=False) #always crawl all online resources (this allows to revisit historic data to the full extent)
    
    #per challenge...
    result_files = []
    for name, sources in all_sources:
        #skip calculation of this subset as the respective online resource could not be crawled       
        if not success_subsets is None and not name in success_subsets:
            rclogger.warning("Skipping calculations for subset "+name+" as not all necessary online resources could be crawled.")
            continue
        try:
            all_incompletes = {}  # dict of methods which have incomplete submissions (per method a list of already submitted benchmarks)
            all_vals = dc.get_joined_dataset(sources, tmp_dir, only_subset=only_subset, read_only=read_only, allow_fuzzy_namecmp = allow_fuzzy_namecmp, renaming_methods=renaming_methods)
            if len(all_vals) <= 0:
                continue # skip this subset, it has no data (on purpose; otherwise get_joined_dataset would throw an exception)
            #one large csv containing all the data
            all_keys, all_rankings = dc.get_all_rankings(all_vals)
            header_list = [dc.column_id] + sorted(list(all_rankings))
            header_list += sorted(list(set(all_keys) - set(header_list)))
            dc.save_as_csv(header_list, all_vals.keys(), all_vals, tmp_dir + "/all_%s.csv" % name, order_name="idx", column_id=dc.column_id)
            filtered_vals, incomplete_vals = dc.remove_incomplete(all_vals, all_rankings, white_list, return_incompletes=True)

            # first fill incompletes even for benchmarks without submissions
            subset_ranking_names = []
            for s in sources:
                if s.format_subset() is None:  # this source is only archived for historical data reasons and is not used for acquiring rankings
                    continue
                check_prefix = dc.rank_prefix + '_' + s.name()
                sub_join_name = name_joined_rank + "_" + s.name()
                subset_ranking_names.append(sub_join_name)
                for m, vals in incomplete_vals.items():
                    if any([k for k in vals.keys() if k.find(check_prefix) == 0]):
                        all_incompletes.setdefault(m,{}).update({sub_join_name: fake_incomplete_rank})
                        all_incompletes[m][dc.column_id] = vals.get(dc.column_id,m)

            #gather only the intersection between all datasets
            dc.save_as_csv(header_list, filtered_vals.keys(), filtered_vals, tmp_dir + "/filtered_%s.csv" % name, order_name="idx", column_id=dc.column_id)

            all_sources_calculated = True
            subset_result_files = []  # result files per source; these are only appended to result_files if all calculations of all associated datasources worked

            if len(filtered_vals.keys()) > 0 :
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
                    sub_join_name = name_joined_rank + "_" + src_name
                    check_prefix = dc.rank_prefix + '_' + s.name()

                    if len(subjoined_ranking) <= 0:
                        rclogger.warning("Skipping calculations for subset "+name+", could not calculate joined ranking for dataset "+src_name)
                        all_sources_calculated = False
                        continue

                    sub_join_file = tmp_dir + "/subjoined_%s.csv" % src_name
                    #add new subset ranking to filtered_vals
                    filtered_vals = dc.add_new_ranking(filtered_vals, dc.column_id, sub_join_name, subjoined_ranking)
                    display_vals = dc.normalize_rankings(filtered_vals, src_rankings, add_old_rank = True)
                    dc.save_as_csv(header_list_subset, subjoined_ranking, display_vals, sub_join_file, order_name=sub_join_name, column_id=dc.column_id)
                    subset_result_files.append(sub_join_file)
            else:
                rclogger.warning("Datasets of subset "+name+ " do not share any common methods; skipping rank joining..")
                all_sources_calculated = False
                if len(all_incompletes) == 0:
                    rclogger.warning("No incomplete submissions for subset " + name + "; skipping output...")
                    continue #do not generate empty file

            joined_ranking = []
            if all_sources_calculated:
                rclogger.info("Calculating joined ranking for subset "+name)
                joined_ranking = get_joined_ranking(filtered_vals, dc.column_id, subset_ranking_names, batchsize = batchsize_calc, max_calc= max_sort_calc, weight_p_ranking = {}, rclogger = rclogger)
            else:
                for s in sources:
                    if s.format_subset() is None: # this source is only archived for historical data reasons and is not used for acquiring rankings
                        continue
                    sub_join_name = name_joined_rank + "_" + s.name()
                    check_prefix = dc.rank_prefix + '_' + s.name()
                    for m, vals in filtered_vals.items():
                        if any([k for k in vals.keys() if k.find(check_prefix) == 0]):
                            all_incompletes.setdefault(m, {}).update({sub_join_name: fake_incomplete_rank})
                            all_incompletes[m][dc.column_id] = vals.get(dc.column_id, m)
            # Balance the influence of each dataset (sum of wheights per dataset are the same)
            # wheights = dc.calc_weight_per_benchmark(all_rankings, sources)
            # joined_ranking = get_joined_ranking(list(filtered_vals.values()), dc.column_id, all_rankings,  batchsize = batchsize_calc, max_calc= max_sort_calc, weight_p_ranking = wheights, rclogger = rclogger)
                        
            if len(joined_ranking) > 0:
                # add all submissions as incompletes if subset cannot be calculated due to a malfunctioning/hidden leaderboard
                filtered_vals = dc.add_new_ranking(filtered_vals, dc.column_id, name_joined_rank, joined_ranking)
                overall_res_file = tmp_dir + "/ranked_full_%s.csv" % name
                all_val = [r.replace(dc.rank_prefix+"_","") for r in all_rankings]
                dc.save_as_csv([name_joined_rank, dc.column_id]+subset_ranking_names+all_rankings+all_val, joined_ranking, filtered_vals, overall_res_file, column_id=dc.column_id)
            else:
                rclogger.warning("Skipping calculations for subset "+name+" as no overall joined ranking could be calculated.")

            #this file is a condensed version having the overall joined ranks and the individual joined ranks per dataset
            condensed_res_file = tmp_dir + "/ranked_condensed_%s.csv" % name
            # sort incomplete methods by the number of completed submissions (i.e. show most promising at top)
            if len(all_incompletes) > 0:
                incomplete_sorted_methods = sorted(all_incompletes.items(), key=lambda kv: len(kv[1]), reverse=True)
                rclogger.info("Adding %i incomplete submissions to output for subset "%len(all_incompletes) + name + ".")
                incomplete_sorted = []
                sorted_methods_same = [incomplete_sorted_methods[0][0]]
                sorted_methods_same_len = len(incomplete_sorted_methods[0][1])
                for k in incomplete_sorted_methods[1:][:]+[['last_elem',[]]]:
                    if len(k[1]) < sorted_methods_same_len:
                        if white_list is None and sorted_methods_same_len < 3: #this prevents too many entries during test sessions (i.e. show simply all submissions); "method" is always part of the list -> 3 means limit to 2 datasets
                            break
                        for m in sorted_methods_same:
                            all_incompletes[m][name_joined_rank]=fake_incomplete_rank
                        incomplete_sorted.append(sorted(sorted_methods_same))
                        sorted_methods_same = [k[0]]
                        sorted_methods_same_len = len(k[1])
                        fake_incomplete_rank += 1
                    else:
                        sorted_methods_same.append(k[0])
                filtered_vals.update(all_incompletes)
                joined_ranking += incomplete_sorted

            dc.save_as_csv([name_joined_rank, dc.column_id]+subset_ranking_names, joined_ranking, filtered_vals, condensed_res_file, column_id=dc.column_id)
            
            
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
    
