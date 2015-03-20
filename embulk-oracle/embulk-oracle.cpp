// embulk-oracle.cpp : DLL アプリケーション用にエクスポートされる関数を定義します。
//

#include "stdafx.h"
#include "org_embulk_output_oracle_oci_OCI.h"
#include "DirPathLoad.h"


static OCI_CONTEXT *toContext(JNIEnv *env, jbyteArray contextAddr)
{
	OCI_CONTEXT *context;
	env->GetByteArrayRegion(contextAddr, 0, sizeof(context), (jbyte*)&context);
	return context;
}

JNIEXPORT jbyteArray JNICALL Java_org_embulk_output_oracle_oci_OCI_createContext
  (JNIEnv *env, jobject)
{
	printf("TEST!\r\n");

	OCI_CONTEXT *context = new OCI_CONTEXT();
	jbyteArray contextAddr = env->NewByteArray(sizeof(OCI_CONTEXT*));
	env->SetByteArrayRegion(contextAddr, 0, sizeof(OCI_CONTEXT*), (jbyte*)&context);

	strcpy(context->message, "testcontext");
	return contextAddr;
}


JNIEXPORT jbyteArray JNICALL Java_org_embulk_output_oracle_oci_OCI_getLasetMessage
  (JNIEnv *env, jobject, jbyteArray contextAddr)
{
	OCI_CONTEXT *context = toContext(env, contextAddr);
	jbyteArray message = env->NewByteArray(sizeof(context->message));
	env->SetByteArrayRegion(message, 0, sizeof(context->message), (jbyte*)context->message);
	return message;
}


JNIEXPORT jboolean JNICALL Java_org_embulk_output_oracle_oci_OCI_open
  (JNIEnv *env, jobject, jbyteArray contextAddr, jstring dbNameString, jstring userNameString, jstring passwordString)
{
	OCI_CONTEXT *context = toContext(env, contextAddr);

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
  (JNIEnv *env, jobject, jbyteArray contextAddr, jobject table)
{
	OCI_CONTEXT *context = toContext(env, contextAddr);
	
	jclass tableClass = env->FindClass("Lorg/embulk/output/oracle/oci/TableDefinition;");
	jfieldID tableNameFieldID = env->GetFieldID(tableClass, "tableName", "Ljava/lang/String;");
	jstring tableNameString = (jstring)env->GetObjectField(table, tableNameFieldID);
	const char *tableName = env->GetStringUTFChars(tableNameString, NULL);
	
	jfieldID columnsFieldID = env->GetFieldID(tableClass, "columns", "[Lorg/embulk/output/oracle/oci/ColumnDefinition;");
	jarray columnArray = (jarray)env->GetObjectField(table, columnsFieldID);
	int columnCount = env->GetArrayLength(columnArray);

	jclass columnClass = env->FindClass("Lorg/embulk/output/oracle/oci/ColumnDefinition;");

	COL_DEF *colDefs = new COL_DEF[columnCount];
	for (int i = 0; i < columnCount; i++) {
		//
	}

	//env->GetObjectField

	//prepareDirPathStream(
	printf(tableName);
	printf("tableName");

	delete[] colDefs;
	env->ReleaseStringUTFChars(tableNameString, tableName);

	return true;
}


JNIEXPORT void JNICALL Java_org_embulk_output_oracle_oci_OCI_close
  (JNIEnv *env, jobject, jbyteArray contextAddr)
{
	OCI_CONTEXT *context = toContext(env, contextAddr);
	freeHandles(context);
	delete context;
}
