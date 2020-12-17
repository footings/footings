
API
===

.. currentmodule:: footings

The footings library has a core component and three sub-libraries -

- model_tools
- test_tools
- doc_tools

core
----
.. automodule:: footings
   :exclude-members:

.. autosummary::
   :nosignatures:
   :toctree: generated

   footings.model
   footings.step
   footings.def_parameter
   footings.def_sensitivity
   footings.def_meta
   footings.def_intermediate
   footings.def_return
   footings.dispatch_function
   footings.audit

|

model_tools
-----------

.. automodule:: footings.model_tools
   :exclude-members:

.. autosummary::
   :nosignatures:
   :toctree: generated

   footings.model_tools.create_frame
   footings.model_tools.create_frame_from_record
   footings.model_tools.expand_frame_per_record
   footings.model_tools.frame_add_exposure
   footings.model_tools.frame_add_weights
   footings.model_tools.frame_filter
   footings.model_tools.calculate_age


|

test_tools
-----------

.. automodule:: footings.test_tools
   :exclude-members:

.. autosummary::
   :nosignatures:
   :toctree: generated

   footings.test_tools.assert_footings_files_equal
   footings.test_tools.load_footings_file

|

doc_tools
---------

.. currentmodule:: footings.doc_tools

.. automodule:: footings.doc_tools
   :exclude-members:
