%% Macros for logging with module prefix
-define(LOG(Level, Fmt, Args), logger:Level("[~s] " ++ Fmt, [?MODULE | Args])).

-define(DBG(Fmt, Args), ?LOG(debug, Fmt, Args)).
-define(INF(Fmt, Args), ?LOG(info, Fmt, Args)).
-define(WRN(Fmt, Args), ?LOG(warning, Fmt, Args)).
-define(ERR(Fmt, Args), ?LOG(error, Fmt, Args)).

