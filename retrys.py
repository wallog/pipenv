#!/usr/bin/env python3.6

import time

def retry(func, times=0):
    def wrapper(*args, **kwargs):
        nonlocal times
        while times < 3:
            try:
                func(*args, **kwargs)
            except Exception as e:
                times += 1 
                print(f"[error] {e}, retry {times} times...")
                time.sleep(2)
                #func(*args, **kwargs)
            else:
                break
    return wrapper
