import os

os.chdir(os.path.dirname(__file__) or ".")
from __handlers.hitit import CreateAuthorization

if __name__ == "__main__":
    CreateAuthorization().create()
