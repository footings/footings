
API
===

.. currentmodule:: footings

The footings library has a core component and four sub-libraries -

- footings.model_tools
- footings.parallel_tools
- footings.test_tools
- footings.doc_tools

footings
--------

.. automodule:: footings
   :exclude-members:

.. autosummary::
   :nosignatures:
   :toctree: generated

   model
   step
   def_parameter
   def_sensitivity
   def_meta
   def_intermediate
   def_return
   dispatch_function
   audit

|

footings.model_tools
--------------------

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
   run_date_time
   once
   convert_to_records


|

footings.parallel_tools
-----------------------

.. automodule:: footings.parallel_tools
   :exclude-members:

.. autosummary::
   :nosignatures:
   :toctree: generated

   WrappedModel
   MappedModel
   ForeachJig
   foreach_jig
   dask.dask_foreach_jig
   ray.ray_foreach_jig


footings.test_tools
-------------------

.. automodule:: footings.test_tools
   :exclude-members:

.. autosummary::
   :nosignatures:
   :toctree: generated

   load_footings_file
   load_footings_json_file
   load_footings_xlsx_file
   assert_footings_files_equal
   assert_footings_json_files_equal
   assert_footings_xlsx_files_equal

|

footins.doc_tools
-----------------

.. currentmodule:: footings.doc_tools

.. automodule:: footings.doc_tools
   :exclude-members:
