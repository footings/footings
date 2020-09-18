
API
===

.. currentmodule:: footings

The footings library is made up of three sub-libraries -

- core
- tools
- doctolls


core
----

.. autosummary::
   :nosignatures:

   Footing
   model
   step
   define_parameter
   define_modifier
   define_meta
   define_placeholder
   define_asset
   dispatch_function

|

.. autoclass:: Footing
.. autofunction:: model
.. autofunction:: step
.. autofunction:: define_parameter
.. autofunction:: define_modifier
.. autofunction:: define_meta
.. autofunction:: define_placeholder
.. autofunction:: define_asset
.. autofunction:: dispatch_function


tools
-----

.. currentmodule:: footings.tools

.. autosummary::
   :nosignatures:

   create_frame
   create_frame_from_record
   expand_frame_per_record
   calculate_age
   post_drop_columns
   assert_footings_audit_xlsx_equal
   load_footings_audit_xlsx

|

.. autofunction:: create_frame
.. autofunction:: create_frame_from_record
.. autofunction:: expand_frame_per_record
.. autofunction:: calculate_age
.. autofunction:: post_drop_columns
.. autofunction:: assert_footings_audit_xlsx_equal
.. autofunction:: load_footings_audit_xlsx


doctools
--------

.. currentmodule:: footings.doctools

.. autosummary::
   :nosignatures:

   docscrape.FootingsDoc

|

.. autoclass:: footings.doctools.docscrape.FootingsDoc
