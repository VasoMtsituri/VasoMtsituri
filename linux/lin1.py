SYSLOG_FILE = '/var/log/syslog'


def file_opener(path):
    with open(path) as f:
        data = f.readlines()

    return data


if __name__ == '__main__':
    logs = file_opener(SYSLOG_FILE)

    print(type(logs))
    print(len(logs))

    for logline in logs[9:10]:
        parts = logline.split()
        print(len(parts))
        print(parts)
        print(logline)