#!/usr/bin/env python
# some automatic test cases for testing functionality of multirankings
 
import os, logging, sys
from multi_ranking import get_joined_ranking
import datacrawler as dc

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    #call test_cases and compare against existing joined rank
    test_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),"../tests")
    test_files=['t0.csv', 't1.csv', 't2.csv', 't3.csv', 't4.csv', 't5.csv', 't6.csv', 't7.csv']
    rclogger = dc.setup_logging(test_dir+'/result_testrun.log')
    batchsize_calc = 10 
    max_sort_calc= 10
    expected_result_name = "joined_"+dc.rank_prefix
    for t in test_files:
        t0 = os.path.join(test_dir,t)
        if not os.path.exists(t0):
            rclogger.error('Cannot open file '+ t0 + '.')
            sys.exit(-8)
        inp_csv = dc.load_from_csv(t0, dc.column_id, None, rclogger)
        all_keys, all_rankings = dc.get_all_rankings(inp_csv, None)
        ranking_names = list(all_keys-set([expected_result_name, dc.column_id])) # do not incorporate expected result during multi-ranking
        res = get_joined_ranking(inp_csv, dc.column_id, ranking_names, batchsize = batchsize_calc, max_calc= max_sort_calc, weight_p_ranking = {}, rclogger = rclogger)
        if expected_result_name not in all_keys:
            res_csv = dc.add_new_ranking(inp_csv, dc.column_id, expected_result_name, res)
            res_file = 'result_'+t
            rclogger.info('Testcase '+t+' has no comparision values. Saving result as '+ res_file)
            dc.save_as_csv([expected_result_name,dc.column_id]+ranking_names, res, res_csv, os.path.join(test_dir,res_file), None)
            continue
        actual_result_name = 'compare_'+expected_result_name
        res_csv = dc.add_new_ranking(inp_csv, dc.column_id, actual_result_name, res)
        for name, vals in res_csv.iteritems():
            if expected_result_name in vals:
                if not actual_result_name in vals:
                    rclogger.error('Did not find result rank for row '+name+' of '+t)
                    sys.exit(-3)
                try:
                    v0, v1 = int(vals[actual_result_name]), int(vals[expected_result_name])
                except:
                    rclogger.error('Cannot convert '+str(v1)+' of row '+name+' of '+t+' into integer.')
                    sys.exit(-5)
                if v0 != v1:
                    rclogger.error('Rank mismatch for row '+name+ ' ( %i vs. %i)  of %s'%(v0,v1,t))
                    sys.exit(-1)
        rclogger.info("Test "+t+" completed successfully.")
    rclogger.info("All tests completed successfully.")
    sys.exit(0)