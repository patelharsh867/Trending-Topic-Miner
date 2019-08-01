def read_config(file):
    separator = "="
    keys = {}
    with open(file) as f:
        for line in f:
            if separator in line:
                name, value = line.split(separator, 1)
                keys[name.strip()] = value.strip()
    return keys
