
# API
---

<br>

{{footings.argument.create_argument}}

<br>

## create_argument {: #an_id .a_class }
---

Create an argument.

### Parameters

- `name : str` - Name to assign the argument. Will show in docstring of created models.
- `**kwargs` - See Argument details.

### See Also
Argument

### Returns
`Argument` - The created argument.

### Examples
```
create_argument(
    name = "test",
    description = "This is an argument",
    dtype = str,
    default="default"
)
```


{{footings.argument.Argument}}

<br>

## Argument
---

An argument is a representation of a parameter to be passed to model built with the footings framework.

#### Attributes

- `name : str` - Name to assign the argument. Will show in docstring of created models.

- `description : str` - A description of the argument.

- `default : Any, optional` - The default value when a value is not passed to argument in a model.

- `dtype : type, optional` - The expected type of the passed argument value. Will be used as a validator.

- `allowed : List[Any], optional` - A list of values that are allowed. Will be used as a validator.

- `min_val :` - The minimum value allowed. Will be used as a validator.

- `max_val :` - The maximum value allowed. Will be used as a validator.

- `min_len :` - The minimum length allowed. Will be used as a valaidator.

- `max_len :` - The maximum length allowed. Will be used as a validator.

- `custom : callable` - A custom function that acts as a validator.

- `other_meta : dict` - Other meta passed to argument.

{{footings.footing.use}}

{{footings.footing.Dependent}}

{{footings.model.build_model}}

{{footings.utils.DispatchFunction}}

{{footings.utils.LoadedFunction}}
