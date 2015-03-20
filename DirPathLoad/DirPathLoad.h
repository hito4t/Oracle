#include <oci.h>

#define ERROR -1
#define SUCCEEDED 0

typedef struct _OCI_CONTEXT {
	OCIEnv     *env;
	OCIDirPathCtx *dp;
	OCISvcCtx *svc;
	OCIError *err;
	OCIDirPathColArray *dpca;
	OCIDirPathStream *dpstr;
	char *buffer;
	FILE *csv;
	char message[512];
} OCI_CONTEXT;

typedef struct _COL_DEF {
	const char *name;
	ub4 type;
	ub4 size;
} COL_DEF;


int prepareDirPathCtx(OCI_CONTEXT *context, const char *db, const char *user, const char *pass);