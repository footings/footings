
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

   Footing
   Footing.audit
   Footing.run
   Footing.visualize
   model
   step
   define_parameter
   define_sensitivity
   define_meta
   define_intermediate
   define_return
   dispatch_function

|

model_tools
-----------

.. currentmodule:: footings.model_tools

.. automodule:: footings.model_tools
   :exclude-members:

.. autosummary::
   :nosignatures:
   :toctree: generated

   create_frame
   create_frame_from_record
   expand_frame_per_record
   frame_add_exposure
   frame_add_weights
   frame_filter
   calculate_age


|

test_tools
-----------

.. currentmodule:: footings.test_tools

.. automodule:: footings.test_tools
   :exclude-members:

.. autosummary::
   :nosignatures:
   :toctree: generated

   assert_footings_audit_xlsx_equal
   load_footings_audit_xlsx

|

doc_tools
---------

.. currentmodule:: footings.doc_tools

.. automodule:: footings.doc_tools
   :exclude-members:
