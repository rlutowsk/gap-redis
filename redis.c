#include "src/compiled.h"
#include <stdio.h>
#include <stdlib.h>
#include <hiredis/hiredis.h>

#ifdef DEBUG
#define INFO() fprintf( stderr, "%s:%d\n", __FUNCTION__, __LINE__ )
#else
#define INFO() {}
#endif

#ifndef R_NAME_MAX
#define R_NAME_MAX 1024
#endif

redisContext *ctx;
redisReply   *rep;
char         address[R_NAME_MAX+1];
int          port;
Obj          record;
Obj			 rval;
UInt         RC_ERR;
UInt         RC_STR;
UInt         RC_VAL;
char         errstr[R_NAME_MAX+1];

Obj retOO( Obj error, Obj value, Obj string )
{
	record = NEW_PREC( 3 );
	SET_RNAM_PREC( record, 1, RC_ERR );
	SET_RNAM_PREC( record, 2, RC_STR );
	SET_RNAM_PREC( record, 3, RC_VAL );
	SET_LEN_PREC ( record, 3 );

	AssPRec( record, RC_ERR, error  );
	AssPRec( record, RC_VAL, value  );
	AssPRec( record, RC_STR, string );

	SortPRecRNam( record, 1 );

	return record;
}

Obj retOC( Obj error, Obj value, Char *string )
{
	UInt length = strlen( string );
	Obj rstring, rvalue;

	if ( length > R_NAME_MAX ) { length = R_NAME_MAX; }

	rvalue = value;
    rstring = MakeString( string );

	return retOO( error, rvalue, rstring );
}

Obj retIC( Obj error, Int value, Char *string )
{
	Obj rerror, rvalue, rstring;
	UInt length = strlen( string );
	
	if ( length > R_NAME_MAX ) { length = R_NAME_MAX; }

	rerror = error;
	rvalue = INTOBJ_INT( value );
	rstring= MakeString( string );

	return retOO( rerror, rvalue, rstring );
}

#define retSuccess retIC( False, 0, "" )
#define retNotConn retIC( True, -1, "No redis server connection" )
#define retBadArgs retIC( True, -1, "Bad function arguments" )

#define CheckCTX(c) if (!RedisConnected()) { return retNotConn; }

int RedisConnected(void)
{
	if ( ctx ) {
		rep = redisCommand( ctx, "PING" );
		if ( rep == NULL ) {
			redisFree( ctx );
			ctx = NULL;
			return 0;
		}
		freeReplyObject( rep );
		return 1;
	}
	return 0;
}

Obj FuncRedisConnected( Obj self )
{
	if ( RedisConnected() ) {
		return retOC( False, True, "" );
	}
	return retOC( False, False, "" );
}

Obj FuncRedisConnect( Obj self, Obj Addr, Obj Port )
{
	if ( ctx ) {
		retIC( True, -1, "Connection already established" );
		return record;
	}

	if ( ! IS_STRING( Addr ) || ! IS_INTOBJ( Port ) ) {
	  return retBadArgs;
	}
	strncpy( address, CSTR_STRING( Addr ), R_NAME_MAX );
	port = INT_INTOBJ( Port );
	ctx  = redisConnect( address, port );
	if ( ctx && ctx->err ) {
		snprintf( errstr, R_NAME_MAX, "Redis connection error: %s", ctx->errstr );
		retIC( True, -1, errstr );
		redisFree( ctx );
		ctx = NULL;
		return record;
	}
	CheckCTX( ctx );
	return retSuccess;
}

Obj FuncRedisFree( Obj self )
{
	redisFree( ctx );
	ctx = NULL;
	
	return retSuccess;
}

Obj FuncRedisZADDElm( Obj self, Obj Set, Obj Score, Obj Elm )
{ 
	CheckCTX( ctx );

	if ( ! IS_STRING( Set ) || ! IS_INTOBJ( Score ) || ! IS_STRING( Elm ) ) {
		return retBadArgs;
	}
	rep = redisCommand( ctx, "ZADD %s %d %s", CSTR_STRING(Set), INT_INTOBJ(Score), CSTR_STRING(Elm) );
	if ( rep->type == REDIS_REPLY_INTEGER ) {
		retIC( False, rep->integer, "" );
	} else {
		retIC( True, -1*rep->type, "Non integer value returned" );
	}
	freeReplyObject( rep );
	return record;
}	

Obj FuncRedisZPOPElm( Obj self, Obj Set, Obj Count )
{
	redisReply *get;
	Obj list, rstring;

	CheckCTX( ctx );

	if ( ! ( IS_STRING( Set ) && IS_INTOBJ(Count) ) ) {
		return retBadArgs;
	}
    rep = redisCommand( ctx, "ZMPOP 1 %s MIN COUNT %d", CSTR_STRING(Set), INT_INTOBJ(Count) );

	if (rep->type==REDIS_REPLY_ARRAY && rep->elements>=2 && rep->element[1]->type==REDIS_REPLY_ARRAY) {
		get = rep->element[1];
		list = NEW_PLIST( T_PLIST, get->elements );
		SET_LEN_PLIST( list, get->elements );
		PLAIN_LIST( list );
		for (int i=0; i<get->elements; i++) {
			rstring = MakeString( get->element[i]->element[0]->str );
			ASS_LIST( list, i+1, rstring );
		}
		retOC( False, list, "");
	} else {
		retIC( True, rep->type, "Error in ZMPOP" );
	}

	/*
	if ( rep->type != REDIS_REPLY_ARRAY ) {
		snprintf( errstr, R_NAME_MAX, "Transaction error: %s", rep->str );
		retIC( True, -1*rep->type, errstr );
	} else {
		if ( rep->elements != 2 ) {
			snprintf( errstr, R_NAME_MAX, "Transaction error - wrong number of operations" );
			retIC( True, -1*rep->type, errstr );
		} else {
			get = rep->element[0];
			rem = rep->element[1];
			if ( get->type != REDIS_REPLY_ARRAY || rem->type != REDIS_REPLY_INTEGER ) {
				snprintf( errstr, R_NAME_MAX, "Transaction error - ZRANGE or ZREM failure" );
				retIC( True, -1, errstr );
			} else if ( get->elements == 0 ) {
				retIC( True, rem->integer, "" );
			} else {
				retIC( False, rem->integer, get->element[0]->str );
			}
		}
	}
	*/
	get = NULL;
	// rem = NULL;
	freeReplyObject( rep );

	return record;
}

Obj FuncRedisZRANGE( Obj self, Obj Set, Obj Start, Obj End )
{
	Obj list, rstring;

	CheckCTX( ctx );
	
	if ( ! ( IS_STRING(Set) && IS_INTOBJ(Start) && IS_INTOBJ(End) ) ) {
		return retBadArgs;
	}
	rep = redisCommand( ctx, "ZRANGE %s %d %d", CSTR_STRING(Set), INT_INTOBJ(Start), INT_INTOBJ(End) );
	if ( rep->type == REDIS_REPLY_ARRAY ) {
		list = NEW_PLIST( T_PLIST, rep->elements );
		SET_LEN_PLIST( list, rep->elements );
		PLAIN_LIST( list );
		for (int i=0; i<rep->elements; i++) {
			rstring = MakeString( rep->element[i]->str );
			ASS_LIST( list, i+1, rstring );
		}
		retOC( False, list, "" );
	} else {
		
	}
	freeReplyObject( rep );
	return record;
}

Obj FuncRedisZRANGEBYSCORE( Obj self, Obj Set, Obj Start, Obj End )
{
	Obj list, rstring;

	CheckCTX( ctx );
	
	if ( ! ( IS_STRING(Set) && IS_INTOBJ(Start) && IS_INTOBJ(End) ) ) {
		return retBadArgs;
	}
	rep = redisCommand( ctx, "ZRANGEBYSCORE %s %d %d", CSTR_STRING(Set), INT_INTOBJ(Start), INT_INTOBJ(End) );
	if ( rep->type == REDIS_REPLY_ARRAY ) {
		list = NEW_PLIST( T_PLIST, rep->elements );
		SET_LEN_PLIST( list, rep->elements );
		PLAIN_LIST( list );
		for (int i=0; i<rep->elements; i++) {
			rstring = MakeString( rep->element[i]->str );
			ASS_LIST( list, i+1, rstring );
		}
		retOC( False, list, "" );
	} else {
		retIC( True, rep->type, "Error in ZRANGEBYSCORE" );
	}
	freeReplyObject( rep );
	return record;
}

Obj FuncRedisPing( Obj self )
{
	CheckCTX();

	if ( ctx ) {
		rep = redisCommand( ctx, "PING" );
		if ( rep->type == REDIS_REPLY_STATUS ) {
			retIC( False, 0, rep->str );
		} else {
			retIC( True, -1*rep->type, "Return is not a status type" );
		}
		return record;
	}
	retIC( True, -1, "Not connected" );
	return record;
}

Obj FuncZREM( Obj self, Obj Set, Obj Elm )
{
	CheckCTX( ctx );

	if ( ! ( IS_STRING( Set ) && IS_STRING( Elm ) ) ) {
		return retBadArgs;
	}

	rep = redisCommand( ctx, "ZREM %s %s", CSTR_STRING(Set), CSTR_STRING(Elm) );
	if ( rep->type == REDIS_REPLY_INTEGER ) {
		retIC( False, rep->integer, "" );
	} else {
		retIC( True, rep->type, "Error in ZREM" );
	}
	freeReplyObject( rep );

	return record;
}

Obj FuncZREMScore( Obj self, Obj Set, Obj ScoreMin, Obj ScoreMax )
{
	CheckCTX( ctx );

	if ( ! ( IS_STRING( Set ) && IS_INTOBJ( ScoreMin ) && IS_INTOBJ( ScoreMax ) ) ) {
		return retBadArgs;
	}
	rep = redisCommand( ctx, "ZREMRANGEBYSCORE %s %d %d", CSTR_STRING( Set ), INT_INTOBJ( ScoreMin ), INT_INTOBJ( ScoreMax ) );
	if ( rep->type == REDIS_REPLY_INTEGER ) {
		retIC( False, rep->integer, "" );
	} else {
		retIC( True, rep->type, "Error in ZREMRANGEBYSCORE" );
	}
	freeReplyObject( rep );
	return record;
}

Obj FuncZCOUNT( Obj self, Obj Set, Obj ScoreMin, Obj ScoreMax )
{
  CheckCTX( ctx );

  if ( ! ( IS_STRING( Set ) && IS_INTOBJ( ScoreMin ) && IS_INTOBJ( ScoreMax ) ) ) {
    return retBadArgs;
  }
  rep = redisCommand( ctx, "ZCOUNT %s %d %d", CSTR_STRING( Set ), INT_INTOBJ( ScoreMin ), INT_INTOBJ( ScoreMax ) );
  if ( rep->type == REDIS_REPLY_INTEGER ) {
    retIC( False, rep->integer, "" );
  } else {
    retIC( True, rep->type, "Error in ZREMRANGEBYSCORE" );
  }
  freeReplyObject( rep );
  return record;
}

Obj FuncSetCounter(Obj self, Obj Name, Obj Value)
{
	CheckCTX( ctx );

	if ( ! ( IS_STRING( Name ) && IS_INTOBJ( Value ) ) ) {
    	return retBadArgs;
  	}

	redisCommand( ctx, "SET %s %d", CSTR_STRING( Name ), INT_INTOBJ( Value ));
	retIC( False, INT_INTOBJ( Value ), "");

	return (Obj)0;
}

Obj FuncGetCounter(Obj self, Obj Name)
{
    CheckCTX( ctx );

    if ( ! IS_STRING( Name ) ) {
    	return retBadArgs;
  	}

	rep = redisCommand( ctx, "GET %s", CSTR_STRING( Name ));
	if ( rep->type == REDIS_REPLY_STRING ) {
		rval = INTOBJ_INT(atoi(rep->str));
	} else {
		rval = Fail;
	}
	freeReplyObject(rep);
	return rval;
}

Obj FuncINCR( Obj self, Obj Name )
{
	CheckCTX( ctx );

	if ( ! IS_STRING( Name ) ) {
    	return retBadArgs;
  	}

	rep = redisCommand( ctx, "INCR %s", CSTR_STRING( Name ));
	if ( rep->type == REDIS_REPLY_INTEGER ) {
		rval = INTOBJ_INT(rep->integer);
	} else {
		rval = Fail;
	}
	freeReplyObject(rep);
	return rval;
}

Obj FuncINCRBY( Obj self, Obj Name, Obj Value )
{
	CheckCTX( ctx );

	if ( ! (IS_STRING( Name ) && IS_INTOBJ(Value)) ) {
    	ErrorQuit("Bad argument types", 0L, 0L);
  	}

	rep = redisCommand( ctx, "INCRBY %s %d", CSTR_STRING( Name ), INT_INTOBJ(Value));
	if ( rep->type == REDIS_REPLY_INTEGER ) {
		rval = INTOBJ_INT(rep->integer);
	} else {
		rval = Fail;
	}
	freeReplyObject(rep);
	return rval;
}

Obj FuncPUSH(Obj self, Obj LR, Obj Name, Obj Data)
{
	if ( ! ( IS_INTOBJ(LR) ) && IS_STRING( Name ) && IS_STRING( Data ) ) {
    	ErrorQuit("Bad argument types", 0L, 0L);
  	}
	if (INT_INTOBJ(LR)==0) {
		/* push from the left */
		rep = redisCommand(ctx, "LPUSH %s %s", CSTR_STRING(Name), CSTR_STRING(Data));
	} else {
		rep = redisCommand(ctx, "RPUSH %s %s", CSTR_STRING(Name), CSTR_STRING(Data));
	}
	if ( rep->type == REDIS_REPLY_INTEGER ) {
		rval = INTOBJ_INT(rep->integer);
	} else {
		rval = Fail;
	}
	freeReplyObject(rep);
	return rval;
}

Obj FuncPOP(Obj self, Obj LR, Obj Name)
{
	if ( ! ( IS_INTOBJ(LR) ) && IS_STRING( Name ) ) {
    	ErrorQuit("Bad argument types", 0L, 0L);
  	}
	if (INT_INTOBJ(LR)==0) {
		/* push from the left */
		rep = redisCommand(ctx, "LPOP %s", CSTR_STRING(Name));
	} else {
		rep = redisCommand(ctx, "RPOP %s", CSTR_STRING(Name));
	}
	if ( rep->type == REDIS_REPLY_STRING ) {
		rval = MakeString(rep->str);
	} else {
		rval = Fail;
	}
	freeReplyObject(rep);
	return rval;
}

Obj FuncDEL(Obj self, Obj Name)
{
	if ( !IS_STRING( Name ) ) {
    	ErrorQuit("Bad argument types", 0L, 0L);
  	}
	rep = redisCommand(ctx, "DEL %s", CSTR_STRING(Name));
	return (Obj)0;
}

/* 
 * GVarFunc - list of functions to export
 */
static StructGVarFunc GVarFunc[] = {
    { "C_REDIS_CONNECT"      , 2, " address, port"        , FuncRedisConnect      , "redis.c:RedisConnect"           },
    { "C_REDIS_FREE"         , 0, ""                      , FuncRedisFree         , "redis.c:RedisFree"              },
    { "C_REDIS_PING"         , 0, ""                      , FuncRedisPing         , "redis.c:RedisPing"              },
    { "C_REDIS_ZRANGE"       , 3, " set, start, end "     , FuncRedisZRANGE       , "redis.c:FuncRedisZRANGE"        },
    { "C_REDIS_ZRANGEBYSCORE", 3, " set, start, end "     , FuncRedisZRANGEBYSCORE, "redis.c:FuncRedisZRANGEBYSCORE" },
    { "C_REDIS_ZADD_ELM"     , 3, " set, score, element " , FuncRedisZADDElm      , "redis.c:RedisZADDElm"           }, 
    { "C_REDIS_ZPOP_ELM"     , 2, " set, count "          , FuncRedisZPOPElm      , "redis.c:RedisZPOPElm"           },
    { "C_REDIS_ZREM"         , 2, " set, element "        , FuncZREM              , "redis.c:RedisZREM"              },
    { "C_REDIS_ZREM_SCORE"   , 3, " set, min, max "       , FuncZREMScore         , "redis.c:RedisZREMScore"         },
    { "C_REDIS_ZCOUNT"       , 3, " set, min, max "       , FuncZCOUNT            , "redis.c:RedisSCOUNT"            },
    { "C_REDIS_CONNECTED"    , 0, ""                      , FuncRedisConnected    , "redis.c:RedisConnected"         },
	{ "RedisSetCounter"      , 2, " name, value"          , FuncSetCounter        , "redis.c:SetCounter"             },
    { "RedisGetCounter"      , 1, " name"                 , FuncGetCounter        , "redis.c:GetCounter"             },
	{ "RedisIncrement"       , 1, " name"                 , FuncINCR              , "redis.c:Increment"              },
	{ "RedisIncrementBy"     , 2, " name, value"          , FuncINCRBY            , "redis.c:IncrementBy"            },
	{ "RedisLRPush"          , 3, " lr, name, data"       , FuncPUSH              , "redis.c:LRPush"                 },
	{ "RedisLRPop"           , 2, " lr, name"             , FuncPOP               , "redis.c:LRPop"                  },
	{ "RedisDelete"          , 1, " name"                 , FuncDEL               , "redis.c:Delete"                 },
    { 0 }
};

static Int InitKernel (StructInitInfo * module)
{
    InitHdlrFuncsFromTable(GVarFunc);
    return 0;
}

static Int InitLibrary(StructInitInfo * module)
{
    InitGVarFuncsFromTable(GVarFunc);

    ctx = NULL;
    rep = NULL;

    RC_ERR = RNamName("err");
    RC_STR = RNamName("str");
    RC_VAL = RNamName("val");

    return 0;
}

static StructInitInfo module = {
    MODULE_DYNAMIC,
    "shared",
    0,
    0,
    0,
    0,
    InitKernel,
    InitLibrary,
    0,
    0,
    0,
    0
};

StructInitInfo * Init__Dynamic(void)
{
  return &module;
}
