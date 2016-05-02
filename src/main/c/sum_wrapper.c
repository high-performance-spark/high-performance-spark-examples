#include "sum.h"
#include "include/com_highperformancespark_example_SumJNI_SumJNI.h"
#include <jni.h>

/*
 * Class:     com_highperformancespark_example_SumJNI_SumJNI
 * Method:    sum
 * Signature: ([I)I
 */
JNIEXPORT jint JNICALL Java_com_highperformancespark_example_SumJNI_SumJNI_sum
(JNIEnv *env, jobject obj, jintArray ja) {
  jsize size = (*env)->GetArrayLength(env, ja);
  jint *a = (*env)->GetIntArrayElements(env, ja, 0);
  return sum(a, size);
}
