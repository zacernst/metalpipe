'''
Decorator that eliminates the need to assign attributes to the keys
and values of kwargs in an ``__init__`` function.
'''
import inspect


class set_kwarg_attributes:

    def __init__(self, exclude=None):
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


if __name__ == '__main__':
    # Example usage

    class Foo:

        @set_kwarg_attributes()
        def __init__(self, thing, foo='bar', baz='qux'):
            print('hi')

        def whatever(self):
            return 'hi'

    foo = Foo('hithere', foo='baz')
