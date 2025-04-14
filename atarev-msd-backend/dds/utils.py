from collections import Counter


def return_days(x):
    currX = [e for e in x]
    all_days = [1, 2, 3, 4, 5, 6, 7]
    conv = {1: 'M', 2: 'T', 3: 'W', 4: 'T', 5: 'F', 6: 'S', 7: 'S'}
    ret = []
    for e in all_days:
        if e not in currX:
            ret.append('-')
        else:
            ret.append(conv[e])
    return ' '.join(ret)


def get_most_popular(x):
    try:
        return Counter([e for e in x.split('|') if e.strip() and e.strip() != '-']).most_common()[0][0]
    except:
        return '-'


def most_popular(x):
    currX = [e if type(e) == str or type(e) == int else 'Direct' for e in x]
    return Counter(currX).most_common(1)[0][0]


def list_to_str(x):
    class_conv = {'XQM Class': ('FIRST', 0), 'GQF Class': ('BUS', 1), 'RWG Class': ('PRE', 2), 'QNI Class': ('ECO', 3),
                  'SDO Class': ('ECO', 4), 'Economy': ('ECO', 4)}

    try:
        currX = [class_conv[e] for e in x if e != 'NKB Class']
        return '-'.join([e[0] for e in sorted(set(currX), key=lambda t: t[1])])
    except:
        return '-'


def duration_to_str(x):
    val = x.to_list()[0]
    if len(str(val)) % 2 != 0:
        val = f"0{val}"
    else:
        val = str(val)

    suffix = 'h' if len(val) > 2 else 'm'
    return f"{val[0:2]}h {val[2:]}" if len(val) > 2 else f"{val[0:2]}{suffix}"
