use std::slice;

use jni::JNIEnv;
use jni::objects::JClass;
use jni::sys::{jint, jlong};

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_org_bvolpato_query_jni_L02_1RustLib_add(
    _env: JNIEnv,
    _class: JClass,
    num1: jint,
    num2: jint,
) -> jint {
    return num1 + num2;
}


#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_org_bvolpato_query_jni_L02_1RustLib_multiply(
    _env: JNIEnv,
    _class: JClass,
    memory_address: jlong,
    size: jint,
) -> jlong {
    let raw_pointer = memory_address as *const i32;
    let slice = unsafe { slice::from_raw_parts(raw_pointer, size as usize) };

    slice.iter().map(|&num| num as i64).reduce(|num1, num2| num1 * num2).unwrap_or(1)
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_org_bvolpato_query_jni_L02_1RustLib_sum(
    _env: JNIEnv,
    _class: JClass,
    memory_address: jlong,
    size: jint,
) -> jlong {
    let raw_pointer = memory_address as *const i32;
    let slice = unsafe { slice::from_raw_parts(raw_pointer, size as usize) };

    slice.iter().map(|&num| num as i64).sum()
}
