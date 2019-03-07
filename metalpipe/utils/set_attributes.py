"""
Decorator that eliminates the need to assign attributes to the keys
and values of kwargs in an ``__init__`` function.
"""

import inspect


class set_kwarg_attributes:
    """
    Function decorator to be used on ``__init__`` methods. It examines the
    signature of the ``__init__`` method and any kwargs passed to the
    constructor. It creates attributes named after the keys, and assigns
    the corresponding values to those attributes.

    For example:

    ::

        class Foo:
            @set_kwarg_attributes()
            def __init__(self, bar='bar', bar='baz'):
                pass

    Calling ``bar = Foo()``, you'd find that ``bar.bar == 'bar`` and
    ``bar.bar == 'baz'``, without having to go through the tedious torture
    of explicitly writing boilerplate like ``self.bar = bar``.

    You can also exclude some kwargs from this magic by passing a list
    ``exclude=[...]`` to the decorator.
    """

    def __init__(self, exclude=None):
        """
        Constructor for the decorator.

        Args:
            exclude (list of str): Names of kwargs that will **not** be
                assigned to attributes of the object.
        """
        self.exclude = exclude or []

    def __call__(self, f):
        def inner_function(_self, *args, **kwargs):
            updated_kwargs = {}
            for name, value in inspect.signature(f).parameters.items():
                if not isinstance(value.default, type):
                    updated_kwargs[name] = value.default
            updated_kwargs.update(kwargs)
            for kwarg, value in updated_kwargs.items():
                if kwarg in self.exclude:
                    continue
                setattr(_self, kwarg, value)
            f(_self, *args, **kwargs)

        return inner_function
