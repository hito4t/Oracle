// embulk-oracle.cpp : DLL アプリケーション用にエクスポートされる関数を定義します。
//

#include "stdafx.h"
#include "org_embulk_output_oracle_oci_OCI.h"
#include "DirPathLoad.h"


static OCI_CONTEXT *toContext(JNIEnv *env, jbyteArray addrs)
{
	OCI_CONTEXT *context;
	env->GetByteArrayRegion(addrs, 0, sizeof(context), (jbyte*)&context);
	return context;
}

static COL_DEF *toColDefs(JNIEnv *env, jbyteArray addrs)
{
	COL_DEF *colDefs;
	env->GetByteArrayRegion(addrs, sizeof(OCI_CONTEXT*), sizeof(colDefs), (jbyte*)&colDefs);
	return colDefs;
}

JNIEXPORT jbyteArray JNICALL Java_org_embulk_output_oracle_oci_OCI_createContext
  (JNIEnv *env, jobject)
{
	printf("TEST!\r\n");

	OCI_CONTEXT *context = new OCI_CONTEXT();
	jbyteArray addrs = env->NewByteArray(sizeof(OCI_CONTEXT*) + sizeof(COL_DEF*));
	env->SetByteArrayRegion(addrs, 0, sizeof(OCI_CONTEXT*), (jbyte*)&context);

	strcpy(context->message, "testcontext");
	return addrs;
}


JNIEXPORT jbyteArray JNICALL Java_org_embulk_output_oracle_oci_OCI_getLasetMessage
  (JNIEnv *env, jobject, jbyteArray addrs)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	jbyteArray message = env->NewByteArray(sizeof(context->message));
	env->SetByteArrayRegion(message, 0, sizeof(context->message), (jbyte*)context->message);
	return message;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_open
  (JNIEnv *env, jobject, jbyteArray addrs, jstring dbNameString, jstring userNameString, jstring passwordString)
{
	OCI_CONTEXT *context = toContext(env, addrs);

	const char *dbName = env->GetStringUTFChars(dbNameString, NULL);
	const char *userName = env->GetStringUTFChars(userNameString, NULL);
	const char *password = env->GetStringUTFChars(passwordString, NULL);

	int result = prepareDirPathCtx(context, dbName, userName, password);

	env->ReleaseStringUTFChars(dbNameString, dbName);
	env->ReleaseStringUTFChars(userNameString, userName);
	env->ReleaseStringUTFChars(passwordString, password);

	if (result != SUCCEEDED) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_prepareLoad
  (JNIEnv *env, jobject, jbyteArray addrs, jobject table)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	
	jclass tableClass = env->FindClass("Lorg/embulk/output/oracle/oci/TableDefinition;");
	jfieldID tableNameFieldID = env->GetFieldID(tableClass, "tableName", "Ljava/lang/String;");
	jstring tableNameString = (jstring)env->GetObjectField(table, tableNameFieldID);
	const char *tableName = env->GetStringUTFChars(tableNameString, NULL);
	
	jfieldID columnsFieldID = env->GetFieldID(tableClass, "columns", "[Lorg/embulk/output/oracle/oci/ColumnDefinition;");
	jobjectArray columnArray = (jobjectArray)env->GetObjectField(table, columnsFieldID);
	int columnCount = env->GetArrayLength(columnArray);

	jclass columnClass = env->FindClass("Lorg/embulk/output/oracle/oci/ColumnDefinition;");
	jfieldID columnNameFieldID = env->GetFieldID(columnClass, "columnName", "Ljava/lang/String;");
	jfieldID columnTypeFieldID = env->GetFieldID(columnClass, "columnType", "I");
	jfieldID columnSizeFieldID = env->GetFieldID(columnClass, "columnSize", "I");

	COL_DEF *colDefs = new COL_DEF[columnCount + 1];
	for (int i = 0; i < columnCount; i++) {
		jobject column = env->GetObjectArrayElement(columnArray, i);
		jstring columnName = (jstring)env->GetObjectField(column, columnNameFieldID);
		colDefs[i].name = env->GetStringUTFChars(columnName, NULL);
		colDefs[i].type = env->GetIntField(column, columnTypeFieldID);
		colDefs[i].size = env->GetIntField(column, columnSizeFieldID);
	}
	colDefs[columnCount].name = NULL;

	int result = prepareDirPathStream(context, tableName, colDefs);

	for (int i = 0; i < columnCount; i++) {
		jobject column = env->GetObjectArrayElement(columnArray, i);
		jstring columnName = (jstring)env->GetObjectField(column, columnNameFieldID);
		env->ReleaseStringUTFChars(columnName, colDefs[i].name);
		//colDefs[i].name = NULL;
	}

	env->SetByteArrayRegion(addrs, sizeof(OCI_CONTEXT*), sizeof(colDefs), (jbyte*)&colDefs);

	//delete[] colDefs;
	env->ReleaseStringUTFChars(tableNameString, tableName);

	if (result != SUCCEEDED) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_loadBuffer
  (JNIEnv *env, jobject, jbyteArray addrs, jbyteArray buffer, jint rowCount)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	COL_DEF *colDefs = toColDefs(env, addrs);

	int result = loadBuffer(context, colDefs, (const char*)env->GetByteArrayElements(buffer, NULL), rowCount);

	if (result != SUCCEEDED) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_commit
  (JNIEnv *env, jobject, jbyteArray addrs)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	if (commit(context) !=SUCCEEDED) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_rollback
  (JNIEnv *env, jobject, jbyteArray addrs)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	if (rollback(context) !=SUCCEEDED) {
		return JNI_FALSE;
	}

	return JNI_TRUE;
}


JNIEXPORT void JNICALL Java_org_embulk_output_oracle_oci_OCI_close
  (JNIEnv *env, jobject, jbyteArray addrs)
{
	OCI_CONTEXT *context = toContext(env, addrs);
	if (context != NULL) {
		freeHandles(context);
		delete context;
	}

	COL_DEF *colDefs = toColDefs(env, addrs);
	if (colDefs != NULL) {
		delete[] colDefs;
	}

}
