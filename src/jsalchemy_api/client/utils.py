import re
from time import sleep
from typing import Callable

CAP_WORD = re.compile(r'[A-Z][a-z]')

def kebab_case(camel: str) -> str:
    """Transform any canel case string into a kebab case"""
    ret = CAP_WORD.sub(lambda x: f'-{x.group().lower()}', camel).lower()
    return ret[1:] if ret.startswith('-') else ret


def wait_until(condition: Callable):
    while condition():
        sleep(0.05)

def deep_scan(dct, path: str = ''):
    if not isinstance(dct, (list, tuple, dict)):
        yield path, dct  # yield the content
    else:  # it's a container
        key_template, iterator = ('{}.{}', dct.items()) if isinstance(dct, dict) else ('{}[{}]', enumerate(dct))
        for key, value in iterator:
            yield from deep_scan(value, key_template.format(path, key) if path else key)  # recursion

def deep_diff(left, right, ignore_left=False, ignore_right=False, flat=False):
    if not flat:
        left, right = (dict(deep_scan(x)) for x in (left, right))
    a_keys, b_keys = map(set, (left, right))
    ao_keys, bo_keys = a_keys - b_keys, b_keys - a_keys
    ret = {}
    if not ignore_left:
        ret = {k: (left[k], None) for k in ao_keys}
    if not ignore_right:
        ret.update({k: (None, right[k]) for k in bo_keys})
    ret.update({k: (left[k], right[k]) for k in set(left).intersection(right) if left[k] != right[k]})
    return ret
