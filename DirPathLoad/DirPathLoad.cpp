#include "stdafx.h"
#include <string.h>
#include <stdio.h>
#include <malloc.h>
#include <time.h>
#include "DirPathLoad.h"


static int check(OCI_CONTEXT *context, const char* message, sword result)
{
	strcpy(context->message, "");

	if (result == OCI_ERROR) {
		sprintf(context->message, "OCI : %s failed.", message);
		sb4 errCode;
		OraText text[512];
		if (OCIErrorGet(context->err, 1, NULL, &errCode, text, sizeof(text) / sizeof(OraText), OCI_HTYPE_ERROR) != OCI_SUCCESS) {
			strcat(context->message, " OCIErrorGet failed.");
		} else {
			//printf("%d : %s\r\n", errCode, text);
			strcat(context->message, " ");
			strncat(context->message, (const char*)text, sizeof(context->message) - strlen(context->message) - 1);
		}
		return ERROR;
	}

	if (result == OCI_INVALID_HANDLE) {
		sprintf(context->message, "OCI : %s failed : invalid handle.", message);
		return ERROR;
	}

	if (result == OCI_NO_DATA) {
		sprintf("OCI : %s failed : no data.", message);
		return ERROR;
	}

	if (result != OCI_SUCCESS) {
		sprintf("OCI : %s failed : %d.", message, result);
		return ERROR;
	}

	return SUCCEEDED;
}

void freeHandles(OCI_CONTEXT *context)
{
	if (context->csv != NULL) fclose(context->csv);
	if (context->buffer != NULL) free(context->buffer);
	if (context->dpstr != NULL) OCIHandleFree(context->dpstr, OCI_HTYPE_DIRPATH_STREAM);
	if (context->dpca != NULL) OCIHandleFree(context->dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY);
	if (context->dp != NULL) OCIHandleFree(context->dp, OCI_HTYPE_DIRPATH_CTX);
	if (context->svc != NULL) OCIHandleFree(context->svc, OCI_HTYPE_SVCCTX);
	if (context->err != NULL) OCIHandleFree(context->err, OCI_HTYPE_ERROR);
	if (context->env != NULL) OCIHandleFree(context->env, OCI_HTYPE_ENV);
}

int prepareDirPathCtx(OCI_CONTEXT *context, const char *dbName, const char *userName, const char *password)
{
	if (check(context, "OCIEnvCreate", OCIEnvCreate(&context->env, 
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
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_ERROR)", OCIHandleAlloc(
		context->env, 
		(void **)&context->err,
        OCI_HTYPE_ERROR,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}

	// サービス・コンテキスト
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_SVCCTX)", OCIHandleAlloc(
		context->env, 
		(void **)&context->svc,
        OCI_HTYPE_SVCCTX,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}

	// ログオン
	if (check(context, "OCILogon", OCILogon(context->env,
		context->err,
		&context->svc,
		(const OraText*)userName,
		strlen(userName),
		(const OraText*)password,
		strlen(password),
		(const OraText*)dbName, // tnsnames.oraを見に行く。"host:port/db"の形式であればtnsnames.ora不要。
		strlen(dbName)))) {
		return ERROR;
	}

	// ダイレクト・パス・コンテキスト
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_DIRPATH_CTX)", OCIHandleAlloc(
		context->env, 
		(void **)&context->dp,
        OCI_HTYPE_DIRPATH_CTX,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}

	return SUCCEEDED;
}

static int isValid(COL_DEF &colDef) {
	return colDef.type != 0;
}

int prepareDirPathStream(OCI_CONTEXT *context, const char *tableName, COL_DEF *colDefs) {
	// ロードオブジェクト名
	if (check(context, "OCIAttrSet(OCI_ATTR_NAME)", OCIAttrSet(context->dp, OCI_HTYPE_DIRPATH_CTX, (void*)tableName, strlen(tableName), OCI_ATTR_NAME, context->err))) {
		return ERROR;
	}
	ub2 cols;
	for (cols = 0; isValid(colDefs[cols]); cols++) ;
	if (check(context, "OCIAttrSet(OCI_ATTR_NUM_COLS)", OCIAttrSet(context->dp, OCI_HTYPE_DIRPATH_CTX, &cols, sizeof(ub2), OCI_ATTR_NUM_COLS, context->err))) {
		return ERROR;
	}

	OCIParam *columns;
	if (check(context, "OCIAttrGet(OCI_ATTR_LIST_COLUMNS)", OCIAttrGet(context->dp, OCI_HTYPE_DIRPATH_CTX, &columns, (ub4*)0, OCI_ATTR_LIST_COLUMNS, context->err))) {
		return ERROR;
	}

	for (int i = 0; i < cols; i++) {
		COL_DEF &colDef = colDefs[i];
		OCIParam *column;
		if (check(context, "OCIParamGet(OCI_DTYPE_PARAM)", OCIParamGet(columns, OCI_DTYPE_PARAM, context->err, (void**)&column, i + 1))) {
			return ERROR;
		}
		if (check(context, "OCIAttrSet(OCI_ATTR_NAME)", OCIAttrSet(column, OCI_DTYPE_PARAM, (void*)colDef.name, strlen(colDef.name), OCI_ATTR_NAME, context->err))) {
			return ERROR;
		}
		if (check(context, "OCIAttrSet(OCI_ATTR_DATA_TYPE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDef.type, sizeof(ub4), OCI_ATTR_DATA_TYPE, context->err))) {
			return ERROR;
		}
		if (check(context, "OCIAttrSet(OCI_ATTR_DATA_SIZE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDef.size, sizeof(ub4), OCI_ATTR_DATA_SIZE, context->err))) {
			return ERROR;
		}
		/*
		if (check(context, "OCIAttrSet(OCI_ATTR_PRECISION)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDefs[i].precision, sizeof(ub4), OCI_ATTR_PRECISION, context->err))) {
			return ERROR;
		}
		if (check(context, "OCIAttrSet(OCI_ATTR_SCALE)", OCIAttrSet(column, OCI_DTYPE_PARAM, &colDefs[i].scale, sizeof(ub4), OCI_ATTR_SCALE, context->err))) {
			return ERROR;
		}
		*/

		if (colDef.dateFormat != NULL) {
			if (check(context, "OCIAttrSet(OCI_ATTR_DATEFORMAT)", OCIAttrSet(column, OCI_DTYPE_PARAM, (void*)colDef.dateFormat, strlen(colDef.dateFormat), OCI_ATTR_DATEFORMAT, context->err))) {
				return ERROR;
			}
		}

		if (check(context, "OCIDescriptorFree(OCI_DTYPE_PARAM)", OCIDescriptorFree(column, OCI_DTYPE_PARAM))) {
			return ERROR;
		}
	}

	if (check(context, "OCIDirPathPrepare", OCIDirPathPrepare(context->dp, context->svc, context->err))) {
		return ERROR;
	}

	// ダイレクト・パス列配列
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_DIRPATH_COLUMN_ARRAY)", OCIHandleAlloc(
		context->dp, 
		(void **)&context->dpca,
        OCI_HTYPE_DIRPATH_COLUMN_ARRAY,
		(size_t)0,
		(void **)0))) {
		return ERROR;
	}

	// ダイレクト・パス・ストリーム
	if (check(context, "OCIHandleAlloc(OCI_HTYPE_DIRPATH_STREAM)", OCIHandleAlloc(
		context->dp, 
		(void **)&context->dpstr,
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

static int loadRows(OCI_CONTEXT *context, ub4 rowCount)
{
	for (ub4 offset = 0; offset < rowCount;) {
		if (check(context, "OCIDirPathStreamReset", OCIDirPathStreamReset(context->dpstr, context->err))) {
			return ERROR;
		}

		sword result = OCIDirPathColArrayToStream(context->dpca, context->dp, context->dpstr, context->err, rowCount, offset);
		if (result != OCI_SUCCESS && result != OCI_CONTINUE) {
			check(context, "OCIDirPathColArrayToStream", result);
			return ERROR;
		}

		if (check(context, "OCIDirPathLoadStream", OCIDirPathLoadStream(context->dp, context->dpstr, context->err))) {
			return ERROR;
		}

		if (result == OCI_SUCCESS) {
			offset = rowCount;
		} else {
			ub4 temp;
			if (check(context, "OCIAttrGet(OCI_ATTR_ROW_COUNT)", OCIAttrGet(context->dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &temp, 0, OCI_ATTR_ROW_COUNT, context->err))) {
				return ERROR;
			}
			offset += temp;
		}
	}

	return SUCCEEDED;
}


int loadBuffer(OCI_CONTEXT *context, COL_DEF *colDefs, const char *buffer, int rowCount)
{
	time_t time0, time1;
	time(&time0);
	clock_t clock0 = clock(), clock1;
	//printf("Start at : %s\r\n", ctime(&time0));
	long csvTime = 0;
	long loadTime = 0;

	ub4 maxRowCount = 0;
	if (check(context, "OCIAttrGet(OCI_ATTR_NUM_ROWS)", OCIAttrGet(context->dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &maxRowCount, 0, OCI_ATTR_NUM_ROWS, context->err))) {
		return ERROR;
	}
	//printf("OCI_HTYPE_DIRPATH_COLUMN_ARRAY.OCI_ATTR_NUM_ROWS = %d\r\n", maxRowCount);

	int rowSize = 0;
	for (int col = 0; isValid(colDefs[col]); col++) {
		rowSize += colDefs[col].size;
	}
	const char *current = buffer;

	int colArrayRowCount = 0;
	for (int row = 0; row < rowCount; row++) {
		for (int col = 0; isValid(colDefs[col]); col++) {
			ub4 size = colDefs[col].size;
			if (colDefs[col].type == SQLT_CHR) {
				size = strnlen(current, size);
			}

			if (check(context, "OCIDirPathColArrayEntrySet", OCIDirPathColArrayEntrySet(context->dpca, context->err, colArrayRowCount, col, (ub1*)current, size, OCI_DIRPATH_COL_COMPLETE))) {
				return ERROR;
			}
			current += colDefs[col].size;
		}

		colArrayRowCount++;
		if (colArrayRowCount == maxRowCount) {
			clock1 = clock();
			csvTime += clock1 - clock0;

			if (loadRows(context, colArrayRowCount)) {
				return ERROR;
			}

			clock0 = clock();
			loadTime += clock0 - clock1;

			colArrayRowCount = 0;
		}
	}

	if (colArrayRowCount > 0) {
		clock1 = clock();
		csvTime += clock1 - clock0;

		if (loadRows(context, colArrayRowCount)) {
			return ERROR;
		}

		clock0 = clock();
		loadTime += clock0 - clock1;
	}

	time(&time1);
	//printf("End at : %s (csv : %d ms, load : %d ms)\r\n", ctime(&time1), csvTime, loadTime);

	return SUCCEEDED;
}


int loadCSV(OCI_CONTEXT *context, COL_DEF *colDefs, const char *csvFileName)
{
	time_t time0, time1;
	time(&time0);
	clock_t clock0 = clock(), clock1;
	printf("Start at : %s\r\n", ctime(&time0));
	long csvTime = 0;
	long loadTime = 0;

	if ((context->csv = fopen(csvFileName, "r")) == NULL) {
		return ERROR;
	}

	ub4 maxRowCount = 0;
	if (check(context, "OCIAttrGet(OCI_ATTR_NUM_ROWS)", OCIAttrGet(context->dpca, OCI_HTYPE_DIRPATH_COLUMN_ARRAY, &maxRowCount, 0, OCI_ATTR_NUM_ROWS, context->err))) {
		return ERROR;
	}
	printf("OCI_HTYPE_DIRPATH_COLUMN_ARRAY.OCI_ATTR_NUM_ROWS = %d\r\n", maxRowCount);

	int rowSize = 0;
	for (int i = 0; isValid(colDefs[i]); i++) {
		rowSize += colDefs[i].size;
	}

	if ((context->buffer = (char*)malloc(rowSize * maxRowCount)) == NULL) {
		return ERROR;
	}
	char *current = context->buffer;

	// TODO:1000文字以上の行は想定していない
	char line[1000];
	int row = 0;
	while (fgets(line, sizeof(line), context->csv) != NULL) {
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

			if (check(context, "OCIDirPathColArrayEntrySet", OCIDirPathColArrayEntrySet(context->dpca, context->err, row, col, (ub1*)current, size, OCI_DIRPATH_COL_COMPLETE))) {
				return ERROR;
			}

			p = next;
			current += size;
			col++;
		}

		row++;
		if (row == maxRowCount) {
			clock1 = clock();
			csvTime += clock1 - clock0;

			if (loadRows(context, row)) {
				return ERROR;
			}

			clock0 = clock();
			loadTime += clock0 - clock1;

			current = context->buffer;
			row = 0;
		}
	}

	if (row > 0) {
		clock1 = clock();
		csvTime += clock1 - clock0;

		if (loadRows(context, row)) {
			return ERROR;
		}

		clock0 = clock();
		loadTime += clock0 - clock1;
	}

	free(context->buffer);
	context->buffer = NULL;

	fclose(context->csv);
	context->csv = NULL;

	time(&time1);
	printf("End at : %s (csv : %d ms, load : %d ms)\r\n", ctime(&time1), csvTime, loadTime);

	return SUCCEEDED;
}

int commit(OCI_CONTEXT *context) 
{
	if (check(context, "OCIDirPathFinish", OCIDirPathFinish(context->dp, context->err))) {
		return ERROR;
	}

	if (check(context, "OCILogoff", OCILogoff(context->svc, context->err))) {
		return ERROR;
	}

	return SUCCEEDED;
}

int rollback(OCI_CONTEXT *context) 
{
	if (check(context, "OCIDirPathFinish", OCIDirPathAbort(context->dp, context->err))) {
		return ERROR;
	}

	if (check(context, "OCILogoff", OCILogoff(context->svc, context->err))) {
		return ERROR;
	}

	return SUCCEEDED;
}


static int test(OCI_CONTEXT *context, const char *db, const char *user, const char *pass, const char *csvFileName)
{
	if (prepareDirPathCtx(context, db, user, pass)) {
		return ERROR;
	}

	COL_DEF colDefs[] = {
		{"ID", SQLT_INT, 4},
		//{"ID", SQLT_CHR, 8},
		{"NUM", SQLT_INT, 4},
		//{"NUM", SQLT_CHR, 12},
		{"VALUE1", SQLT_CHR, 60},
		{"VALUE2", SQLT_CHR, 60},
		{"VALUE3", SQLT_CHR, 60},
		{"VALUE4", SQLT_CHR, 60},
		{"VALUE5", SQLT_CHR, 60},
		{"VALUE6", SQLT_CHR, 60},
		{"VALUE7", SQLT_CHR, 60},
		{"VALUE8", SQLT_CHR, 60},
		{"VALUE9", SQLT_CHR, 60},
		{"VALUE10", SQLT_CHR, 60},
		{NULL, 0, 0}
	};
	if (prepareDirPathStream(context, "EXAMPLE", colDefs)) {
		return ERROR;
	}

	if (loadCSV(context, colDefs, csvFileName)) {
		return ERROR;
	}

	if (commit(context)) {
		return ERROR;
	}

	return SUCCEEDED;
}


int main(int argc, char* argv[])
{
	if (argc < 5) {
		printf("DirPathLoad <db> <user> <password> <csv file name>\r\n");
		return ERROR;
	}

	OCI_CONTEXT context;
	memset(&context, 0, sizeof(OCI_CONTEXT));
	int result = test(&context, argv[1], argv[2], argv[3], argv[4]);
	if (result == ERROR) {
		printf("%s\r\n", context.message);
	}
	freeHandles(&context);
	return result;
}

