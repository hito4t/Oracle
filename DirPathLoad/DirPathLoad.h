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


int prepareDirPathCtx(OCI_CONTEXT *context, const char *dbName, const char *userName, const char *password);

int prepareDirPathStream(OCI_CONTEXT *context, const char *tableName, COL_DEF *colDefs);
	
int loadBuffer(OCI_CONTEXT *context, COL_DEF *colDefs, const char *buffer, int rowCount);
	
int commit(OCI_CONTEXT *context);
	
int rollback(OCI_CONTEXT *context);
	
void freeHandles(OCI_CONTEXT *context);
