# Contributing

## Setup

Start by cloning the repository:

```sh
git clone https://github.com/Bl3f/yato.git
cd yato
```

Install [uv](https://docs.astral.sh/uv/) and set up the environment:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

uv will automatically install the required Python version (3.9+) if needed.

## Testing

You can run tests once the environment is set up:

```sh
uv run pytest
```
