# import tomli

# with open("../configs/my_config.toml", 'rb') as f:
#     config = tomli.load(f)

# print(config)

import secrets

# Generate a secure secret key
secret_key = secrets.token_urlsafe(32)  # You can adjust the byte size as needed
print(secret_key)
