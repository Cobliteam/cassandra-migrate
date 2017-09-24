try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader


class YamlUnicodeLoader(SafeLoader):
    def construct_yaml_str(self, node):
        return self.construct_scalar(node)


YamlUnicodeLoader.add_constructor(
    'tag:yaml.org,2002:str', YamlUnicodeLoader.construct_yaml_str)
