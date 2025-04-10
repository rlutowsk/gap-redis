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

static Obj   status;
static Obj	 rval;
static UInt  RNAM_ADDRESS,
			 RNAM_PORT,
			 RNAM_REPLY,
			 RNAM_MESSAGE;

static Obj REDIS_REPLY_ERROR_OBJ;
static const char *redis_no_ctx_msg = "Redis context object not allocated";
static Obj REDIS_NO_CTX_MSG;

static Int GVAR_REDIS_STATUS;

inline static void 	SetStatusHostname(const Obj Hostname) 
{
	AssPRec(status, RNAM_ADDRESS, Hostname);
}
inline static void SetStatusPort(const Obj Port)
{
	AssPRec(status, RNAM_PORT, Port);
}
inline static void SetStatusReply(const Obj Reply)
{
	AssPRec(status, RNAM_REPLY, Reply);
}
inline static void SetStatusReplyInt(const Int Reply)
{
	AssPRec(status, RNAM_REPLY, INTOBJ_INT(Reply) );
}
inline static void SetStatusMessage(const Obj Message)
{
	AssPRec(status, RNAM_MESSAGE, Message);
}
inline static void SetStatusMessageCstr(const char *Message)
{
	AssPRec(status, RNAM_MESSAGE, MakeString(Message) );
}

/* check if ctx object is not null */
/* TODO: should we check for more? */
#define CheckCTX() if (!ctx) { return Fail; }
#define ErrorBadArguments() ErrorQuit("Bad arguments provided", 0L, 0L)

Obj FuncRedisConnected( Obj self )
{
	if (ctx) {
		return True;
	}
	SetStatusReply( REDIS_REPLY_ERROR_OBJ );
	SetStatusMessage( REDIS_NO_CTX_MSG );
	return False;
}

Obj FuncRedisConnect( Obj self, Obj Hostname, Obj Port )
{
	if ( ctx ) {
		SetStatusReply(REDIS_REPLY_ERROR_OBJ);
		SetStatusMessageCstr( "Redis context already allocated" );
		return Fail;
	}

	if ( ! IS_STRING( Hostname ) || ! IS_INTOBJ( Port ) ) {
		ErrorBadArguments();
	}
	SetStatusHostname( Hostname );
	SetStatusPort( Port );
	ctx  = redisConnect( CSTR_STRING(Hostname), INT_INTOBJ(Port) );
	if ( ctx && ctx->err ) {
		SetStatusReply( REDIS_REPLY_ERROR_OBJ );
		SetStatusMessage( MakeString(ctx->errstr) );
		return Fail;
	}
	CheckCTX();
	return True;
}

Obj FuncRedisFree( Obj self )
{
	redisFree( ctx );
	ctx = NULL;
	
	return (Obj)0;
}

Obj FuncRedisPing( Obj self )
{
	CheckCTX();

	rep = redisCommand( ctx, "PING" );
	SetStatusReplyInt(rep->type);
	if ( rep->type == REDIS_REPLY_STATUS ) {
		return MakeString( rep->str );
	}
	return Fail;
}

Obj FuncSetCounter(Obj self, Obj Name, Obj Value)
{
	CheckCTX();

	if ( ! ( IS_STRING( Name ) && IS_INTOBJ( Value ) ) ) {
    	ErrorBadArguments();
  	}

	rep = redisCommand( ctx, "SET %s %d", CSTR_STRING( Name ), INT_INTOBJ( Value ));
	SetStatusReplyInt(rep->type);

	freeReplyObject(rep);
	return (Obj)0;
}

Obj FuncGetCounter(Obj self, Obj Name)
{
    CheckCTX();

    if ( ! IS_STRING( Name ) ) {
    	ErrorBadArguments();
  	}

	rep = redisCommand( ctx, "GET %s", CSTR_STRING( Name ));
	SetStatusReplyInt(rep->type);

	if ( rep->type == REDIS_REPLY_STRING ) {
		rval = MakeStringWithLen( rep-> str, rep->len );
	} else {
		SetStatusMessageCstr( rep->str );
		rval = Fail;
	}

	freeReplyObject(rep);
	return rval;
}

Obj FuncINCR( Obj self, Obj Name )
{
	CheckCTX();

	if ( ! IS_STRING( Name ) ) {
    	ErrorBadArguments();
  	}

	rep = redisCommand( ctx, "INCR %s", CSTR_STRING( Name ));
	SetStatusReplyInt(rep->type);

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
	CheckCTX();

	if ( ! (IS_STRING( Name ) && IS_INTOBJ(Value)) ) {
    	ErrorBadArguments();
  	}

	rep = redisCommand( ctx, "INCRBY %s %d", CSTR_STRING( Name ), INT_INTOBJ(Value));
	SetStatusReplyInt(rep->type);

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
    	ErrorBadArguments();
  	}
	if (INT_INTOBJ(LR)==0) {
		/* push from the left */
		rep = redisCommand(ctx, "LPUSH %s %s", CSTR_STRING(Name), CSTR_STRING(Data));
	} else {
		rep = redisCommand(ctx, "RPUSH %s %s", CSTR_STRING(Name), CSTR_STRING(Data));
	}
	SetStatusReplyInt(rep->type);

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
    	ErrorBadArguments();
  	}
	if (INT_INTOBJ(LR)==0) {
		/* push from the left */
		rep = redisCommand(ctx, "LPOP %s", CSTR_STRING(Name));
	} else {
		rep = redisCommand(ctx, "RPOP %s", CSTR_STRING(Name));
	}
	SetStatusReplyInt(rep->type);

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
    	ErrorBadArguments();
  	}
	rep = redisCommand(ctx, "DEL %s", CSTR_STRING(Name));
	SetStatusReplyInt(rep->type);

	return (Obj)0;
}

Obj FuncCMD(Obj self, Obj Cmd)
{
	CheckCTX();

	if ( !IS_STRING( Cmd ) ) {
    	ErrorBadArguments();
  	}
	rep = redisCommand(ctx, CSTR_STRING(Cmd));
	SetStatusReplyInt(rep->type);

	switch(rep->type) {
		case REDIS_REPLY_STATUS:
			rval = MakeStringWithLen( rep-> str, rep->len );
			break;
		case REDIS_REPLY_ERROR:
			SetStatusMessage( MakeStringWithLen(rep->str, rep->len) );
			rval = Fail;
			break;
		case REDIS_REPLY_INTEGER:
			rval = INTOBJ_INT( rep->integer );
			break;
		case REDIS_REPLY_NIL:
			rval = (Obj)0;
			break;
		case REDIS_REPLY_VERB:
			/* this should probably do more ... */
		case REDIS_REPLY_STRING:
			rval = MakeStringWithLen( rep-> str, rep->len );
			break;
		case REDIS_REPLY_ARRAY:
		case REDIS_REPLY_SET:    /* these two will return a list */
			rval = NEW_PLIST( T_PLIST, rep->elements );
			SET_LEN_PLIST( rval, rep->elements );
			PLAIN_LIST( rval );
			for (size_t i=0; i<rep->elements; i++) {
				// TODO: check if this line is needed
				// rstring = MakeString( rep->element[i]->str );
				ASS_LIST( rval, i+1, MakeString( rep->element[i]->str ) );
			}
			break;
		case REDIS_REPLY_DOUBLE:
			rval = NEW_MACFLOAT( strtod( rep->str, NULL ) );
			break;
		case REDIS_REPLY_BOOL:
			rval = (rep->integer)?True:False;
			break;
		case REDIS_REPLY_PUSH:
			/* left for now */
			SetStatusMessage(MakeString("PUSH response not implemented yet"));
			rval = Fail;
			break;
		case REDIS_REPLY_MAP:    /* return a record */
			rval = NEW_PREC(0);
			for (size_t i=0; i<rep->elements; i += 2) {
				AssPRec(rval, RNamName(rep->element[i]->str), MakeString(rep->element[i+1]->str));
			}
			break;
		case REDIS_REPLY_BIGNUM:
			rval = IntStringInternal( NULL, rep->str );
			break; 
	}
	return rval;
}

/* 
 * GVarFunc - list of functions to export
 */
static StructGVarFunc GVarFunc[] = {
    { "RedisConnect"         , 2, " address, port"        , FuncRedisConnect      , "redis.c:RedisConnect"           },
    { "RedisFree"            , 0, ""                      , FuncRedisFree         , "redis.c:RedisFree"              },
    { "RedisPing"            , 0, ""                      , FuncRedisPing         , "redis.c:RedisPing"              },
    { "RedisConnected"       , 0, ""                      , FuncRedisConnected    , "redis.c:RedisConnected"         },
	{ "RedisSetCounter"      , 2, " name, value"          , FuncSetCounter        , "redis.c:SetCounter"             },
    { "RedisGetCounter"      , 1, " name"                 , FuncGetCounter        , "redis.c:GetCounter"             },
	{ "RedisIncrement"       , 1, " name"                 , FuncINCR              , "redis.c:Increment"              },
	{ "RedisIncrementBy"     , 2, " name, value"          , FuncINCRBY            , "redis.c:IncrementBy"            },
	{ "RedisLRPush"          , 3, " lr, name, data"       , FuncPUSH              , "redis.c:LRPush"                 },
	{ "RedisLRPop"           , 2, " lr, name"             , FuncPOP               , "redis.c:LRPop"                  },
	{ "RedisDelete"          , 1, " name"                 , FuncDEL               , "redis.c:Delete"                 },
	{ "RedisCommand"         , 1, " command_string"       , FuncCMD               , "redis.c:Command"                },
    { 0 }
};

static Int InitKernel (StructInitInfo * module)
{
    InitHdlrFuncsFromTable(GVarFunc);
    return 0;
}

static Int InitLibrary(StructInitInfo * module)
{
	int gvar;

    InitGVarFuncsFromTable(GVarFunc);

    ctx = NULL;
    rep = NULL;

	REDIS_NO_CTX_MSG      = MakeString(redis_no_ctx_msg);
	REDIS_REPLY_ERROR_OBJ = INTOBJ_INT(REDIS_REPLY_ERROR);

	RNAM_ADDRESS = RNamName("hostname");
	RNAM_PORT    = RNamName("port");
	RNAM_REPLY   = RNamName("reply");
	RNAM_MESSAGE = RNamName("message");

	status = NEW_PREC(0);
	AssPRec(status, RNAM_ADDRESS, MakeString(""));
	AssPRec(status, RNAM_PORT   , INTOBJ_INT(0));
	AssPRec(status, RNAM_REPLY  , INTOBJ_INT(0));
	AssPRec(status, RNAM_MESSAGE, MakeString(""));

	GVAR_REDIS_STATUS = GVarName("REDIS_STATUS");
	AssGVar(GVAR_REDIS_STATUS, status);
	MakeReadOnlyGVar( GVAR_REDIS_STATUS );

	/* define RESP2 and RESP3 answer values in GAP session */
	gvar = GVarName("REDIS_REPLY_STATUS");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_STATUS) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_ERROR");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_ERROR) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_INTEGER");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_INTEGER) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_NIL");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_NIL) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_STRING");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_STRING) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_ARRAY");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_ARRAY) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_DOUBLE");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_DOUBLE) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_BOOL");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_BOOL) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_MAP");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_MAP) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_SET");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_SET) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_PUSH");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_PUSH) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_ATTR");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_ATTR) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_BIGNUM");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_BIGNUM) );
	MakeReadOnlyGVar( gvar );

	gvar = GVarName("REDIS_REPLY_VERB");
	AssGVar(gvar, INTOBJ_INT(REDIS_REPLY_VERB) );
	MakeReadOnlyGVar( gvar );

    return 0;
}

static StructInitInfo module = {
    .type = MODULE_DYNAMIC,
    .name = "redis",
    .initKernel = InitKernel,
    .initLibrary = InitLibrary,
};

StructInitInfo * Init__Dynamic(void)
{
  return &module;
}
