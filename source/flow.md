Flow
====

Initialization and startup:

1. Child class initialization.
1. `NanoNode` init called (with `super`), passing `*arga` and `**kwargs`.
1. Call `node.global_start()`
1. `node.global_start()` calls `node.start()` for each connected node.
1. If it is defined, `node.get_runtime_attrs(*self.get_runtime_attrs_args,
   **self.get_runtime_attrs_kwargs)` is called.
1. `node.get_runtime_attrs` returns a dictionary-like object. Keys and
   values are stored in `node` as attributes. If
   `node.runtime_attrs_destinations` is defined, then it is used to rename
   the attributes.
1. `node.setup()` is called. The purpose is to do any setup that requires
   attributes that are defined at runtime by the `get_runtime_attrs` method.
1. If `node.is_source`, then call `node.generator()`. Yield `(output, None,)`.
1. If `node.is_source` is `False`, then loop over each `input_queue`. Get item,
   passing if item is `None`. Sleep for `self.throttle` seconds. 
1. Yield results of `node.process_item()`, along with the input message.


