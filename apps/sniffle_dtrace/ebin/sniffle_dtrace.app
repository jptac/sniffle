{application,sniffle_dtrace,
             [{description,"DTrace aggregator"},
              {vsn,"0.1.0"},
              {registered,[]},
              {applications,[kernel,libchunter,stdlib]},
              {mod,{sniffle_dtrace_app,[]}},
              {env,[]},
              {modules,[sniffle_dtrace_app,sniffle_dtrace_server,
                        sniffle_dtrace_sup]}]}.
