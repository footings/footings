
API
===

.. currentmodule:: footings

Core
----

Functions
~~~~~~~~~

.. autosummary::
   :nosignatures:

   define_parameter
   use
   build_model
   dispatch_function
   loaded_function

|
|

.. autofunction:: define_parameter
.. autofunction:: use
.. autofunction:: build_model
.. autofunction:: dispatch_function
.. autofunction:: loaded_function


Classes
~~~~~~~
.. autosummary::
   :nosignatures:

   footings.core.parameter.Parameter
   footings.core.footing.Dependent
   footings.core.model.BaseModel

|
|


.. autoclass:: footings.core.parameter.Parameter
   :members:

.. autoclass:: footings.core.footing.Dependent
   :members:

.. autoclass:: footings.core.model.BaseModel
   :members:

Tools
-----

.. currentmodule:: footings.tools

.. autosummary::
   :nosignatures:

   create_frame
   create_frame_from_record
   expand_frame_per_record
   calculate_age
   post_drop_columns

|
|

.. autofunction:: create_frame
.. autofunction:: create_frame_from_record
.. autofunction:: expand_frame_per_record
.. autofunction:: calculate_age
.. autofunction:: post_drop_columns
