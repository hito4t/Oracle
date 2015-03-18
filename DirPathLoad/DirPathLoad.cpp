#include <string.h>
#include <stdio.h>
#include <oci.h>
#include <malloc.h>
#include <time.h>

#define ERROR -1
#define SUCCEEDED 0

struct HANDLES {
	OCIEnv     *env;
	OCIDirPathCtx *dp;
	OCISvcCtx *svc;
	OCIError *err;
	OCIDirPathColArray *dpca;
	OCIDirPathStream *dpstr;
	char *buffer;
	FILE *csv;
} handles;

struct COL_DEF {
	const char *name;
	ub4 type;
	ub4 size;
	ub4 precision;
	ub4 scale;
};


static int check(const char* message, sword result)
{
	if (result == OCI_ERROR) {
		printf("%s failed.\r\n", message);
		OraText buffer[512];
		sb4 errCode;
		if (OCIErrorGet(handles.err, 1, NULL, &errCode, buffer, 512, OCI_HTYPE_ERROR) != OCI_SUCCESS) {
			printf("OCIErrorGet failed.\r\n");
		} else {
			printf("%d : %s\r\n", errCode, buffer);
		}
		return ERROR;
	}

	if (result == OCI_INVALID_HANDLE) {
		printf("%s failed : invalid handle.\r\n", message);
		return ERROR;
	}

	if (result == OCI_NO_DATA) {
		printf("%s failed : no data.\r\n", message);
		return ERROR;
	}

	if (result != OCI_SUCCESS) {
		printf("%s failed : %d\r\n", message, result);
		return ERROR;
	}

	return SUCCEEDED;
}

static void freeHandles()
{
	if (handles.csv != NULL) fclose(handles.csv);
	if (handles.buffer != NULL) free(handles.buffer);
	if (handles.dpstr != NULL) OCIHandleFree(handles.dpstr, OCI_HTYPE_DIRPATH_STREAM);
	if (handles.dpca != NULL) OCIHandleFree(handles.dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY);
	if (handles.dp != NULL) OCIHandleFree(handles.dp, OCI_HTYPE_DIRPATH_CTX);
	if (handles.svc != NULL) OCIHandleFree(handles.svc, OCI_HTYPE_SVCCTX);
	if (handles.err != NULL) OCIHandleFree(handles.err, OCI_HTYPE_ERROR);
	if (handles.env != NULL) OCIHandleFree(handles.env, OCI_HTYPE_ENV);
}

static int prepareDirPathCtx(const char *db, const char *user, const char *pass)
{
	if (check("OCIEnvCreate", OCIEnvCreate(&handles.env, 
		OCI_THREADED|OCI_OBJECT,
		(void *)0,
		0, 
		0, 
		0,
		(size_t)0, 
		(void **)0))) {
		return ERROR;
	}

	// エラー・ハンドル
	if (check("OCIHandleAlloc(OCI_HTYPE_ERROR)", OCIHandleAlloc(
		handles.env, 
		(void **)&handles.err,
        OCI_HTYPE_ERROR,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}

	// サービス・コンテキスト
	if (check("OCIHandleAlloc(OCI_HTYPE_SVCCTX)", OCIHandleAlloc(
		handles.env, 
		(void **)&handles.svc,
        OCI_HTYPE_SVCCTX,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}

	// ログオン
	if (check("OCILogon", OCILogon(handles.env,
		handles.err,
		&handles.svc,
		(const OraText*)user,
		strlen(user),
		(const OraText*)pass,
		strlen(pass),
		(const OraText*)db,
		strlen(db)))) {
		return ERROR;
	}

	// ダイレクト・パス・コンテキスト
	if (check("OCIHandleAlloc(OCI_HTYPE_DIRPATH_CTX)", OCIHandleAlloc(
		handles.env, 
		(void **)&handles.dp,
        OCI_HTYPE_DIRPATH_CTX,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}

	return SUCCEEDED;
}

static int prepareDirPathStream(const char *table, struct COL_DEF *colDefs) {
	// ロードオブジェクト名
	if (check("OCIAttrSet(OCI_ATTR_NAME)", OCIAttrSet(handles.dp, OCI_HTYPE_DIRPATH_CTX, (void*)table, strlen(table), OCI_ATTR_NAME, handles.err))) {
		return ERROR;
	}
	ub2 cols;
	for (cols = 0; colDefs[cols].name != NULL; cols++) ;

	if (check("OCIAttrSet(OCI_ATTR_NUM_COLS)", OCIAttrSet(handles.dp, OCI_HTYPE_DIRPATH_CTX, &cols, sizeof(ub2), OCI_ATTR_NUM_COLS, handles.err))) {
		return ERROR;
	}

	OCIParam *columns;
	if (check("OCIAttrGet(OCI_ATTR_LIST_COLUMNS)", OCIAttrGet(handles.dp, OCI_HTYPE_DIRPATH_CTX, &columns, (ub4*)0, OCI_ATTR_LIST_COLUMNS, handles.err))) {
		return ERROR;
	}

	for (int i = 0; i < cols; i++) {
		OCIParam *column;
		if (check("OCIParamGet(OCI_DTYPE_PARAM)", OCIParamGet(columns, OCI_DTYPE_PARAM, handles.err, (void**)&column, i + 1))) {
			return ERROR;
		}
		if (check("OCIAttrSet(OCI_ATTR_NAME)", OCIAttrSet(column, OCI_DTYPE_PARAM, (void*)colDefs[i].name, strlen(colDefs[i].name), OCI_ATTR_NAME, handles.err))) {
			return ERROR;
		}
		if (check("OCIAttrSet(OCI_ATTR_DATA_TYPE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDefs[i].type, sizeof(ub4), OCI_ATTR_DATA_TYPE, handles.err))) {
			return ERROR;
		}
		if (check("OCIAttrSet(OCI_ATTR_DATA_SIZE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDefs[i].size, sizeof(ub4), OCI_ATTR_DATA_SIZE, handles.err))) {
			return ERROR;
		}
		if (check("OCIAttrSet(OCI_ATTR_PRECISION)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDefs[i].precision, sizeof(ub4), OCI_ATTR_PRECISION, handles.err))) {
			return ERROR;
		}
		if (check("OCIAttrSet(OCI_ATTR_SCALE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDefs[i].scale, sizeof(ub4), OCI_ATTR_SCALE, handles.err))) {
			return ERROR;
		}

		if (check("OCIDescriptorFree(OCI_DTYPE_PARAM)", OCIDescriptorFree(column, OCI_DTYPE_PARAM))) {
			return ERROR;
		}
	}

	if (check("OCIDirPathPrepare", OCIDirPathPrepare(handles.dp, handles.svc, handles.err))) {
		return ERROR;
	}

	// ダイレクト・パス列配列
	if (check("OCIHandleAlloc(OCI_HTYPE_DIRPATH_COLUMN_ARRAY)", OCIHandleAlloc(
		handles.dp, 
		(void **)&handles.dpca,
        OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}

	// ダイレクト・パス・ストリーム
	if (check("OCIHandleAlloc(OCI_HTYPE_DIRPATH_STREAM)", OCIHandleAlloc(
		handles.dp, 
		(void **)&handles.dpstr,
        OCI_HTYPE_DIRPATH_STREAM,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}
}

static void intToSqlInt(int n, char* buffer)
{
	buffer[0] = n & 0xFF;
	buffer[1] = (n >> 8) & 0xFF;
	buffer[2] = (n >> 16) & 0xFF;
	buffer[3] = (n >> 24) & 0xFF;
}

static int strToSqlInt(const char *s, int size, char* buffer)
{
	int n = 0;
	for (int i = 0; i < size; i++) {
		if (s[i] < '0' || s[i] > '9') {
			return ERROR;
		}
		n = n * 10 + s[i] - '0';
	}

	intToSqlInt(n, buffer);

	return SUCCEEDED;
}

static int loadRows(ub4 rowCount)
{
	for (ub4 offset = 0; offset < rowCount;) {
		sword result = OCIDirPathColArrayToStream(handles.dpca, handles.dp, handles.dpstr, handles.err, rowCount, offset);
		if (result != OCI_SUCCESS && result != OCI_CONTINUE) {
			check("OCIDirPathColArrayToStream", result);
			return ERROR;
		}

		ub4 temp2;
		if (check("OCIAttrGet(OCI_ATTR_ROW_COUNT)", OCIAttrGet(handles.dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &temp2, 0, OCI_ATTR_ROW_COUNT, handles.err))) {
			return ERROR;
		}

		if (check("OCIDirPathLoadStream", OCIDirPathLoadStream(handles.dp, handles.dpstr, handles.err))) {
			return ERROR;
		}

		if (result == OCI_SUCCESS) {
			offset = rowCount;
		} else {
			ub4 temp;
			if (check("OCIAttrGet(OCI_ATTR_ROW_COUNT)", OCIAttrGet(handles.dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &temp, 0, OCI_ATTR_ROW_COUNT, handles.err))) {
				return ERROR;
			}
			offset += temp;
		}

		if (check("OCIDirPathStreamReset", OCIDirPathStreamReset(handles.dpstr, handles.err))) {
			return ERROR;
		}
	}

	return SUCCEEDED;
}


static int loadCSV(struct COL_DEF *colDefs, const char *csvFileName)
{
	time_t time0, time1;
	time(&time0);
	clock_t clock0 = clock(), clock1;
	printf("Start at : %s\r\n", ctime(&time0));
	long csvTime = 0;
	long loadTime = 0;

	if ((handles.csv = fopen(csvFileName, "r")) == NULL) {
		return ERROR;
	}

	ub4 maxRows = 0;
	if (check("OCIAttrGet(OCI_ATTR_NUM_ROWS)", OCIAttrGet(handles.dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &maxRows, 0, OCI_ATTR_NUM_ROWS, handles.err))) {
		return ERROR;
	}
	printf("OCI_HTYPE_DIRPATH_COLUMN_ARRAY.OCI_ATTR_NUM_ROWS = %d\r\n", maxRows);

	int rowSize = 0;
	for (int i = 0; colDefs[i].name != NULL; i++) {
		rowSize += colDefs[i].size;
	}

	if ((handles.buffer = (char*)malloc(rowSize * maxRows)) == NULL) {
		return ERROR;
	}
	char *current = handles.buffer;

	// TODO:1000文字以上の行は想定していない
	char line[1000];
	int row = 0;
	while (fgets(line, sizeof(line), handles.csv) != NULL) {
		int len = strlen(line);
		int col = 0;
		for (const char *p = line; p < line + len;) {
			const char *comma = strchr(p, ',');
			const char *next;
			ub4 size;
			if (comma != NULL) {
				size = comma - p;
				next = comma + 1;
			} else {
				size = line + len - p;
				if (size > 0 && p[size - 1] == '\n') size--;
				if (size > 0 && p[size - 1] == '\r') size--;
				next = line + len;
			}

			if (colDefs[col].type == SQLT_INT) {
				if (strToSqlInt(p, size, current)) {
					printf("Not a number : \"%s\"\r\n", p);
					return ERROR;
				}
				size = colDefs[col].size;
			} else if (colDefs[col].type == SQLT_CHR) {
				strncpy(current, p, size);
			} else {
				printf("Unsupported type : %s\r\n", colDefs[col].type);
				return ERROR;
			}

			if (check("OCIDirPathColArrayEntrySet", OCIDirPathColArrayEntrySet(handles.dpca, handles.err, row, col, (ub1*)current, size, OCI_DIRPATH_COL_COMPLETE))) {
				return ERROR;
			}

			p = next;
			current += size;
			col++;
		}

		row++;
		if (row == maxRows) {
			clock1 = clock();
			csvTime += clock1 - clock0;

			if (loadRows(row)) {
				return ERROR;
			}

			clock0 = clock();
			loadTime += clock0 - clock1;

			current = handles.buffer;
			row = 0;
		}
	}

	if (row > 0) {
		clock1 = clock();
		csvTime += clock1 - clock0;

		if (loadRows(row)) {
			return ERROR;
		}

		clock0 = clock();
		loadTime += clock0 - clock1;
	}

	free(handles.buffer);
	handles.buffer = NULL;

	fclose(handles.csv);
	handles.csv = NULL;

	time(&time1);
	printf("End at : %s (csv : %d ms, load : %d ms)\r\n", ctime(&time1), csvTime, loadTime);

	return SUCCEEDED;
}

static int commit() 
{
	if (check("OCIDirPathFinish", OCIDirPathFinish(handles.dp, handles.err))) {
		return ERROR;
	}

	if (check("OCILogoff", OCILogoff(handles.svc, handles.err))) {
		return ERROR;
	}

	return SUCCEEDED;
}


static int test(const char *db, const char *user, const char *pass, const char *csvFileName)
{
	if (prepareDirPathCtx(db, user, pass)) {
		freeHandles();
		return ERROR;
	}

	struct COL_DEF colDefs[] = {
		{"ID", SQLT_INT, 4, 8, 0},
		//{"ID", SQLT_CHR, 8, 8},
		{"NUM", SQLT_INT, 4, 12, 0},
		//{"NUM", SQLT_CHR, 12, 12},
		{"VALUE1", SQLT_CHR, 60, 60},
		{"VALUE2", SQLT_CHR, 60, 60},
		{"VALUE3", SQLT_CHR, 60, 60},
		{"VALUE4", SQLT_CHR, 60, 60},
		{"VALUE5", SQLT_CHR, 60, 60},
		{"VALUE6", SQLT_CHR, 60, 60},
		{"VALUE7", SQLT_CHR, 60, 60},
		{"VALUE8", SQLT_CHR, 60, 60},
		{"VALUE9", SQLT_CHR, 60, 60},
		{"VALUE10", SQLT_CHR, 60, 60},
		{NULL}
	};
	if (prepareDirPathStream("EXAMPLE", colDefs)) {
		freeHandles();
		return ERROR;
	}

	if (loadCSV(colDefs, csvFileName)) {
		freeHandles();
		return ERROR;
	}

	if (commit()) {
		freeHandles();
		return ERROR;
	}

	freeHandles();

	return SUCCEEDED;
}


int main(int argc, char* argv[])
{
	if (argc < 5) {
		printf("DirPathLoad <db> <user> <password> <csv file name>\r\n");
		return ERROR;
	}

	test(argv[1], argv[2], argv[3], argv[4]);
	return SUCCEEDED;
}

