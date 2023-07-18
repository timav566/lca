![Style Checks](https://github.com/JetBrains-Research/lca/actions/workflows/style-check.yml/badge.svg)

# LCA

## Setup

1. Python 3.9 is required for development
    1. [Install pyenv](https://github.com/pyenv/pyenv#installation)
    2. [Install requirements for Python](https://github.com/pyenv/pyenv#install-python-build-dependencies)
    3. Create Python 3.10 environment:
         ```bash
         pyenv install 3.10
         ```
2. We use Poetry for dependency management
    1. [Install poetry](https://python-poetry.org/docs/#installing-with-the-official-installer)
    2. Tell poetry which Python to use:
        ```bash
        pyenv shell 3.10
        python --version  # ensure that pyenv activated 3.10 version
        poetry env use `which python`
        ```
    3. Finally, you're ready to install dependencies:
         ```bash
         poetry install
         ```