Read("redis.g");

RedisDemoInfo := NewInfoClass("RedisDemo");

SetInfoLevel( RedisDemoInfo, 1 );

TIME_MEMORY_KEY := "time:memory";

TIME_REDIS_START_KEY := "time:redis:start";
TIME_REDIS_END_KEY   := "time:redis:end";

redis_demo_data := [];

# example run: num=10000, dim=50
RedisDemoDeploy := function(num, dim)

    RedisConnect("localhost", 6379);
    Info(RedisDemoInfo, 1, "Connected to redis");

    redis_demo_data := List([1..num], i->rec(name:=i, data:=RandomMat(dim, dim)));
    Info(RedisDemoInfo, 1, "Data generated");
    DeployListLeft("input", redis_demo_data);
    Info(RedisDemoInfo, 1, "Data deployed");
    #Info(RedisDemoInfo, 1, "Calculation time in us: ", RedisGetCounter(TIME_MEMORY_KEY), "\n");
    RedisDelete(TIME_REDIS_START_KEY);
    RedisDelete(TIME_REDIS_END_KEY);

    RedisFree();
end;

RedisDemoRunSingle := function()
    local start, l;
    start:=NanosecondsSinceEpoch(); 
    for l in redis_demo_data do 
        Print(l.name,"\r"); 
        DeterminantIntMat(l.data); 
    od;
    return (NanosecondsSinceEpoch()-start)/1000;
end;

RedisDemoRunPar := function()
    local str, mat;

    RedisConnect("localhost", 6379);

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

RedisDemoRuntime := function()
    local time_start, time_end, str;

    RedisConnect("localhost", 6379);

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

    RedisFree();

    return Maximum(time_end) - Minimum(time_start);
end;
