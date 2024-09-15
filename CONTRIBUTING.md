# Contributing

## Setup

Start by cloning the repository:

```sh
git clone https://github.com/Bl3f/yato.git
```

Next, you'll need a Python environment:

```sh
pyenv install -v 3.12
```

You'll also need [Poetry](https://python-poetry.org/):

```sh
curl -sSL https://install.python-poetry.org | python3 -
poetry install
poetry shell
```

## Testing

You can run tests once the environment is set up:

```sh
pytest
```
