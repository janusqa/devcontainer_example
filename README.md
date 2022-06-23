1. From vscode command palette (crtl+shirf+p) > Remote-Containers: Add Development Container Configuration File...
2. Modify devcointainer.json to your liking. NB: docker-compose used.
   1. Example add extentions to the continer configuration
   2. Example run extra commands like installing requirements.txt for python app
   3. Example foward ports back to the host if your app for instance will launch a listening server
3. From vscode command palette  (crtl+shirf+p) > Remote-Containers: Rebuild and Reopen in Container

# Package environment to ship to executors
- venv-pack --python-prefix "/opt/bitnami/python"
- --python-prefix indicates where to link the executable in the shipped env to the real executable on the executor
- set .env variable PYSPARK_PYTHON=./venv/bin/python (will use this environment in current working directory on executor)