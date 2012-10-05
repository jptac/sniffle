%% app generated at {2012,8,8} {1,58,29}
{application,vmstats,
             [{description,"Tiny application to gather VM statistics for StatsD client"},
              {vsn,"0.1"},
              {id,[]},
              {modules,[vmstats,vmstats_server,vmstats_sup]},
              {registered,[vmstats_sup,vmstats_server]},
              {applications,[statsderl]},
              {included_applications,[]},
              {env,[{delay,5000}]},
              {maxT,infinity},
              {maxP,infinity},
              {mod,{vmstats,[]}}]}.

