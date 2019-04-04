# Contributing to MetalPipe

So you want to contribute? That's great. There's lots to do, and it's still early days.

## Set up your environment

Clone the repo:

``
git clone git@github.com:zacernst/metalpipe.git
``

Inside the `metalpipe` directory, build a virtual environment with Python 3.5+ (3.7 recommended, but we aim to officially support Python >= 3.5).

``
cd metalpipe
python -m venv dev
. ./dev/bin/activate
``

Upgrade pip:

``
pip install --upgrade pip
``

Install all the required packages:

``
pip install -r requirements.txt
``

That ought to be it.

## Contributing code

Contributions can be from any of the listed issues, or not. We use feature branches in the usual way. So the process would be:

1. Pick your battle. Contributions should be small and incremental.
2. Check out a new feature branch, and give it a descriptive name (e.g. `fix_foobar_bug`).
3. Write your code.
4. Add docstrings. We use Google-style docstrings, which are rendered by Sphinx.
5. Add unit tests. See below about how.
6. Push your code and make a pull request.
7. Thank you!

You'll notice that some of these guidelines definitely fall into the "do as I say not as I do" category. Test and docstring coverage isn't great. Any help with those would be very appreciated.

## Unit tests

We use `pytest` for all unit tests. Tests are located in the `./tests` directory. Test functions should have descriptive names, e.g. `test_terminated_nodes_exit`. For our CI process, tests are executed by a top-level script called `./run_tests.sh`. Travis CI runs the tests when a new pull request is opened, and whenever code is merged into `master`.

## Code style

All the code is run through `black`, which does a nice job of formatting everything in a uniform, easy-to-read way. You should run `black` on your code, too. The `black` package is in the `requirements.txt` file, so you should have it already. However, note that `black` requires Python >= 3.6. So although `metalpipe` aims to support Python >= 3.5, `black` doesn't.
