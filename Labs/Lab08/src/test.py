import tomli

with open("../configs/my_config.toml", 'rb') as f:
    config = tomli.load(f)

print(config)