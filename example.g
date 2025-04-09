Read("redis.g");

RedisDemoInfo := NewInfoClass("RedisDemo");

SetInfoLevel( RedisDemoInfo, 1 );

# some default values
REDIS_HOSTNAME := "localhost";
REDIS_PORT     := 6379;

TIME_MEMORY_KEY      := "time:memory";
TIME_REDIS_KEY       := "time:redis";
TIME_REDIS_START_KEY := "time:redis:start";
TIME_REDIS_END_KEY   := "time:redis:end";

QUEUE_INPUT_KEY      := "input";

if not IsBound(redis_demo_data) or Size(redis_demo_data) = 0 then
    redis_demo_data := [];
fi;

RedisDemoSetDefaults := function( r )
    if not IsRecord(r) then
        return;
    fi;
    if IsBound(r.hostname) then
        REDIS_HOSTNAME := r.hostname;
    fi;
    if IsBound(r.port) then
        REDIS_PORT := r.port;
    fi;
end;

RedisDemoGenerateData := function(data...)
    local num, dim;
    
    # first handle defaults:
    if Size(data) = 0 then
        num := 10000;
        dim := 50;
    elif Size(data) = 1 then
        num := data[1];
        dim := 50;
    else
        num := data[1];
        dim := data[2];
    fi;
    Info(RedisDemoInfo, 1, "RD: Generating data ...");
    redis_demo_data := List([1..num], i->rec(name:=i, data:=RandomMat(dim, dim)));
end;

RedisDemoDeploy := function()

    Info(RedisDemoInfo, 1, "RD: Connecting to redis ...");
    RedisConnect(REDIS_HOSTNAME, REDIS_PORT);

    if Size(redis_demo_data) = 0 then
        RedisDemoGenerateData();
    fi;

    Info(RedisDemoInfo, 1, "RD: Deploying data ...");
    DeployListLeft("input", redis_demo_data);
    
    Info(RedisDemoInfo, 1, "RD: Clearing timing data ...");
    RedisDelete(TIME_MEMORY_KEY);
    RedisDelete(TIME_REDIS_KEY);
    RedisDelete(TIME_REDIS_START_KEY);
    RedisDelete(TIME_REDIS_END_KEY);

    RedisFree();
end;

RedisDemoRunSingle := function()
    local time, l;
    if Size(redis_demo_data) = 0 then
        RedisDemoGenerateData();
    fi;
    Info(RedisDemoInfo, 1, "RD: Running single thread demo ... ");
    time:=NanosecondsSinceEpoch(); 
    for l in redis_demo_data do 
        Print(l.name,"\r"); 
        DeterminantIntMat(l.data); 
    od;
    time := (NanosecondsSinceEpoch()-time)/1000;

    RedisConnect(REDIS_HOSTNAME, REDIS_PORT);

    RedisSetCounter(TIME_MEMORY_KEY, time);

    RedisFree();

    return time;
end;

RedisDemoRunPar := function()
    local str, mat;

    RedisConnect(REDIS_HOSTNAME, REDIS_PORT);

    RedisLRPush(0, TIME_REDIS_START_KEY, String(NanosecondsSinceEpoch()/1000));
    while true do
        str := RedisLRPop(1, "input");
        if str = fail then
            break;
        fi;
        mat := EvalString(str);
        DeterminantIntMat(mat.data);
        Print(mat.name, "\r");
    od;
    RedisLRPush(0, TIME_REDIS_END_KEY, String(NanosecondsSinceEpoch()/1000));

    RedisFree();
end;

RedisDemoRuntimes := function()
    local time_start, time_end, t, str;

    t := rec();

    RedisConnect(REDIS_HOSTNAME, REDIS_PORT);

    t.single:= RedisGetCounter(TIME_MEMORY_KEY);
    t.redis := RedisGetCounter(TIME_REDIS_KEY);
    if IsInt(time) and time > 0 then  
        return t;
    fi;

    time_start := [];
    while true do
        str := RedisLRPop(1, TIME_REDIS_START_KEY);
        if str = fail then
            break;
        fi;
        Add(time_start, Int(str));
    od;
    time_end := [];
    while true do
        str := RedisLRPop(1, TIME_REDIS_END_KEY);
        if str = fail then
            break;
        fi;
        Add(time_end, Int(str));
    od;

    t.redis := Maximum(time_end) - Minimum(time_start);
    RedisSetCounter(TIME_REDIS_KEY, t.redis);

    RedisFree();

    return t;
end;
