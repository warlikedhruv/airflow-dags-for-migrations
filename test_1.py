import yaml

import yaml
with open('test_1.yaml') as f:
    my_dict = yaml.safe_load(f)
print(my_dict)