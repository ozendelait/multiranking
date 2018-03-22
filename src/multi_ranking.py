import copy, logging
from pyvotecore import schulze_pr
from pyvotecore import schulze_stv
from pyvotecore import condorcet
from sklearn.metrics import ranking

logging.basicConfig(level=logging.INFO)
def iterative_schulz(input_rankings, num_per_iter, max_calc = -1, tie_breakers=None, rclogger = None):
    if rclogger is None:
        rclogger = logging.getLogger(__name__)
    
    orig_input = copy.deepcopy(input_rankings)
    multi_ranking = []
    all_candidates = set()

    for ranking in input_rankings:
        for elems in ranking["ballot"]:
            all_candidates |= set(elems)
    keep_last_val = set()
    max_num_elems = len(all_candidates)
    open_candidates = set(all_candidates)
    all_winners = {}
    
    for maxpasses in range(max_num_elems):
        rclogger.info("Starting round %i with: %s",  maxpasses , str(open_candidates))
                
        winners_this_round = []
        if max_calc >= 0 and len(all_winners) >= max_calc and not tie_breakers is None:
            #sort the remainder by tie_breakers
            rclogger.info("Ordering remaining open candidates by tie_breaker value")
            open_c_order = []
            last_c = []
            for c in open_candidates:
                if c in tie_breakers:
                    open_c_order.append((c, tie_breakers[c]))
                else:
                    last_c.append(c) #no tie-breaker means last place 
            open_c_order = sorted(open_c_order, key=lambda x: x[1], reverse=True)
            last_score = -1
            for c,score in open_c_order:
                if score != last_score:
                    multi_ranking.append([c])
                    last_score = score
                else:
                    multi_ranking[-1].append(c) #ties with last entry in list
            if len(last_c) > 0:
                multi_ranking.append(last_c)
        elif len(input_rankings) > 0 and len(open_candidates) > 0:
            remaining_ranking = schulze_pr.SchulzePR(input_rankings, ballot_notation=condorcet.CondorcetHelper.BALLOT_NOTATION_GROUPING, winner_threshold=min(num_per_iter, len(open_candidates))).as_dict()
            #remaining_ranking = schulze_stv.SchulzeSTV(input_rankings, ballot_notation=condorcet.CondorcetHelper.BALLOT_NOTATION_GROUPING, winner_threshold=min(num_per_iter, len(open_candidates))).as_dict()
            for k in range(len(remaining_ranking["rounds"])):
                if "tied_winners" in remaining_ranking["rounds"][k]: #there are multiple winners
                    tied_winners = sorted(list(remaining_ranking["rounds"][k]["tied_winners"]))
                    tied_winners_unique = []
                    for w in tied_winners:
                        if not w in all_winners:
                            tied_winners_unique.append(w)
                            all_winners[w] = "tie"
                    if len(tied_winners_unique) > 0:
                        if len(tied_winners_unique) > 1 and not tie_breakers is None:
                            #try to break ties with tie_breakers information
                            for _ in range(len(tied_winners_unique)):
                                if len(tied_winners_unique) < 2:
                                    if len(tied_winners_unique) == 1:
                                        winners_this_round.append(tied_winners_unique)
                                    break
                                max_score = -1
                                for t in tied_winners_unique:
                                    if t in tie_breakers:
                                        max_score = max(max_score, tie_breakers[t])
                                append_winners = []
                                for t in tied_winners_unique:
                                    if not t in tie_breakers or  tie_breakers[t] == max_score:
                                        append_winners.append(t)
                                tied_winners_unique = sorted(list(set(tied_winners_unique)-set(append_winners)))
                                winners_this_round.append(append_winners)
                        else:
                            winners_this_round.append(tied_winners_unique)
                elif "winner" in remaining_ranking["rounds"][k]:
                    cand_winner = remaining_ranking["rounds"][k]["winner"]
                    if not cand_winner in all_winners:
                        all_winners[cand_winner] = "win"
                        winners_this_round.append([cand_winner])
        
        if len(winners_this_round) <= 0:
            if len(keep_last_val) > 0:
                winners_this_round = sorted(list(keep_last_val))
                multi_ranking.append(winners_this_round)
            break
        else:
            multi_ranking += winners_this_round
        
        all_winners_this_round = set()
        for winners in winners_this_round:
            all_winners_this_round |= set(winners)

        open_candidates -= all_winners_this_round

        rclogger.info("Winners of round %i: %s" , maxpasses ,str(winners_this_round))
        
        remaining_rankings = []
        for i in orig_input:
            rem_order = []
            for r in i["ballot"]:
                rem_ties = []
                for t in r:
                    if t in open_candidates:
                        rem_ties.append(t)
                if len(rem_ties) > 0:
                    rem_order.append(rem_ties)
            if len(rem_order) == 1:
                keep_last_val |= set(rem_order[0])
            elif len(rem_order) > 1:
                i["ballot"] = rem_order
                remaining_rankings.append(i)
        input_rankings = remaining_rankings
        orig_input = copy.deepcopy(input_rankings)
    
        all_fixed_candidates = all_candidates - open_candidates
        keep_last_val -= all_fixed_candidates
    rclogger.info("Finished multi-rank joining")
    return multi_ranking

def get_joined_ranking(inp_data, refid, ranking_names, batchsize = 10, max_calc = -1, weight_p_ranking={}, rclogger = None):
    if rclogger is None:
        rclogger = logging.getLogger(__name__)
    if isinstance(inp_data, dict):
        #we expect a list of dicts per entry containing the individual rankings
        inp_data = list(inp_data.values())
    if batchsize < max_calc:
        batchsize = max_calc
        
    all_rankings = {}
    method_rankings = {}
    max_len_rankings = -1 
    for name in ranking_names:
        ranking = []
        curr_weight = weight_p_ranking.get(name,1)
        for vals in inp_data:
            if not refid in vals or not name in vals:
                continue
            val_r = vals[name]
            try:
                val_r = int(val_r)
            except:
                pass
            ranking.append((val_r,vals[refid]))
        #, reverse=True)
        if len(ranking) <= 0:
            rclogger.error("Ranking "+ name + " not found in input data")
            return []
        ranking = sorted(ranking)
        method_ranking = [i[1] for i in ranking]
        method_rankings[name] = method_ranking     
        max_len_rankings = max(max_len_rankings, len(method_ranking))
        id0 = str(method_ranking)
        
        if id0 in all_rankings:
            all_rankings[id0]["count"] += curr_weight
        else:
            list_of_lists = [[str(i)] for i in method_ranking]
            all_rankings[id0] = {"count": curr_weight, "ballot" : list_of_lists}
        
    if len(all_rankings) <= 0:
        rclogger.error("No input rankings.")
        return []        
    
    tie_breaker_score = {}
    for name, method_ranking in method_rankings.iteritems():
        curr_weight = weight_p_ranking.get(name,1)
        for idx,m in enumerate(method_ranking):
            score_add = curr_weight*(max_len_rankings-idx)
            tie_breaker_score[m] = tie_breaker_score.get(m,0)+score_add
    
    
    ranking_ties = sorted(tie_breaker_score.items(), key=lambda x: x[1], reverse=True)
    tie_breakers = [i[0] for i in ranking_ties]
    
    input_rankings = copy.deepcopy(list(all_rankings.values()))
    if batchsize <= 0:
        #original SchulzePR call, this can take very long for larger numbers of methods
        rclogger.info("Calling original SchulzePR code with %i candidates and %i possible rankings %i" , len(tie_breakers) , len(input_rankings))
        multi_ranking = schulze_pr.SchulzePR(input_rankings, ballot_notation=condorcet.CondorcetHelper.BALLOT_NOTATION_GROUPING,tie_breaker=tie_breakers).as_dict()["order"]
    else:
        #iterative version of SchulzePR, only correct for the first batch of max_order_calc entries, after this the results can diverge from the Schulze-Method-Equivalent
        multi_ranking = iterative_schulz(input_rankings, min(len(tie_breaker_score), batchsize), max_calc, tie_breaker_score, rclogger = rclogger)
    return multi_ranking
