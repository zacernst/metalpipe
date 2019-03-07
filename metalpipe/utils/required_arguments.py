"""
A decorator that allows you to specify a set of keyword arguments that
are required by the function. If any are missing, an exception is
raised which lists the missing arguments.

::

    @required_arguments(bar='bar', baz='qux')
    def my_function(x, y, bar=None, baz=None):
        ...something...


Invoking the function like so:

::

    my_function(1, 2, bar='barbar')


will raise:

::

    MissingRequiredArgument: Missing required argument(s): bar

"""


class MissingRequiredArgument(Exception):
    pass


class required_arguments:
    def __init__(self, *kwarg_list):
        self.kwarg_list = kwarg_list

    def __call__(self, f):
        def inner_function(*args, **kwargs):
            missing_kwargs = []
            for kwarg in self.kwarg_list:
                if kwarg not in kwargs:
                    missing_kwargs.append(kwarg)
            if len(missing_kwargs) > 0:
                raise MissingRequiredArgument(
                    "Missing required argument(s): "
                    "{kwarg_list}".format(kwarg_list=", ".join(missing_kwargs))
                )
            out = f(*args, **kwargs)
            return out

        return inner_function
