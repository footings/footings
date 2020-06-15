User Guide
==========

|

Planning out the model
----------------------

This document is intended to be a thought exercise and user guide on how to build models with the
footings framework. Below are a series of items to think about as a model is planned out.

- Objective
- End user arguments
- Structure of the model

Note these items are listed sequentially, though, there is likely to be some iteration between
these items as users sit down to work on them.

**Objective**

When looking to build a model, you first need to define what the objective of the model is.

To demonstrate how to build a model with the footings framework we are going to use the example
objective - **build a simple model to calculate disabled life reserves (DLR)**. A disabled life
reserve is a reserve that is established once a policy holder has qualifying event to receive
future benefit payments. The reserve is the present value of expected future benefit payments
with decrements for claim termination rates.

**End user arguments**

Models need inputs. Inputs can be split into - assumptions and parameters. Typically, assumptions
are built into the model and are called based on parameters that are passed to the model. In addition,
a model parameter can do much more than manage assumption utilization in the model.

The footings framework uses the term argument to define a parameter. A user will need to think about
what arguments they want the user of the model to submit when running the model. In addition, the user
needs to think about -

- What the data type of the arguments are (i.e., date vs string)?
- Are only certain values allowed to be passed (e.g., only accepts values 1 or 2)?
- Should there be a default value when a user does not explicitly set the argument when calling the model?

Continuing with our example of building a DLR model, we will define the following end user arguments -

- Valuation date
- Disabled age
- Gender
- Occupation class
- Elimination period
- Interest rate

**Plan structure of the model**

Given an objective and an idea of what end user arguments are needed, the model developer should start
planing out the structure of the model. The footings framework is built around the principle that
models should be broken down into steps. Thus, the model developer should think about how a model can
be split into small pieces.

Using our example, a DLR model is the present value of a future payment stream decremented by lives
inforce (which is calculated using a claim termination rate (CTR)). So if we think about in terms of
columns in a table, we want the model to produce the following columns -

+-----------------+-------------------+------+---------------+-----------------+----------------+
| Projected Dates | Expected Benefit  | CTR  | Lives Inforce | Discount Factor | PV of Benefits |
+=================+===================+======+===============+=================+================+
| 2019-12-31      | $0                | 0.00 | 1.000         | 1.000           |                |
+-----------------+-------------------+------+---------------+-----------------+----------------+
| 2020-01-31      | $100              | 0.05 | 0.950         | 0.980           |                |
+-----------------+-------------------+------+---------------+-----------------+----------------+
| 2020-02-28      | $100              | 0.06 | 0.892         | 0.960           |                |
+-----------------+-------------------+------+---------------+-----------------+----------------+
| ...             | ...               | ...  | ...           | ...             | ...            |
+-----------------+-------------------+------+---------------+-----------------+----------------+

So working backwards from what we want the model to produce, we can create a separate step in the
model for each one of the columns above.

Building the model
------------------

**Create functions for the defined parts**

Given an objective and a good plan around what the exposed arguments and steps should be. A model
developer can start building. The footings framework builds a model up from  the different steps.
Steps are are represented by functions.

Continuing with the DLR example, below are the functions needed -

.. code-block:: python

    import pandas as pd

    def create_projected_frame(valuation_date, periods):
        dict_ = {
            "DATE": pd.date_range(start=valuation_date, periods=periods, freq="M"),
            "DURATION": range(1, periods + 1)
        }
        return pd.DataFrame(dict_)

    def add_expected_benefit(frame):
        frame["BENEFIT"] = 100
        return frame

    def add_ctr(frame, disabled_age, gender, occ_class, elim_period):
        # assume csv file exists with columns -
        # - DISABLED_AGE
        # - GENDER
        # - OCC_CLASS
        # - ELIM_PERIOD
        # - DURATION
        # - CTR
        #
        # 1. read the data
        # all_ctrs = pd.read_csv("ctr_file.csv")
        #
        # 2. filter to right group
        # use_ctrs = all_ctrs[
        #     all_ctrs["DISABLED_AGE"] == disabled_age,
        #     all_ctrs["GENDER"] == gender,
        #     all_ctrs["OCC_CLASS"] == occ_class,
        #     all_ctrs["ELIM_PERIOD"] == elim_period,
        # ]
        #
        # 3. add CTR to frame
        # frame.merge(use_ctrs[["DURATION", "CTR"]], on=["DURATION"], how="left")

        # for simplicity
        frame["CTR"] = 0.01
        return frame

    def calculate_lives_inforce(frame):
        frame["LIVES"] = (1 - frame["CTR"]).cumprod()
        return frame

    def add_discount_factor(frame, interest_rate):
        frame["DISCOUNT"] = 1 / (1 + interest_rate) ** frame["DURATION"]
        return frame

    def calculate_pv_benefits(frame):
        columns = ["BENEFIT", "LIVES", "DISCOUNT"]
        frame["PV_BENEFITS"] = frame[columns].prod(axis=1).iloc[::-1].cumsum()
        return frame


**Layout structure of the model and draw dependencies**

With the footings framework the steps, which are represented by functions, are arranged in a list
of dicts which is in turn used to create a model. While assembling the list, a developer will draw
the dependencies between the different steps.

Below is what the list would look like using our DLR example -

.. code-block:: python

    from footings import create_argument, use

    # first define arguments
    arg_valuation_date = create_argument(
        name="valuation_date",
        description="The valuation date.",
        dtype=pd.Timestamp
    )
    arg_disabled_age = create_argument(
        name="disabled_age",
        description="The age the policy holder became disabled.",
        dtype=int,
        min_val=20,
        max_val=100
    )
    arg_gender = create_argument(
        name="gender",
        description="The gender of the policy holder.",
        dtype=str,
        allowed=["M", "F"]
    )
    arg_occ_class = create_argument(
        name="occ_class",
        description="The occupation class of the policy holder.",
        dtype=int,
        allowed=[1, 2, 3, 4]
    )
    arg_elim_period = create_argument(
        name="elim_period",
        description="The elimination period for the policy.",
        dtype=int,
        allowed=[0, 7, 14, 30, 60, 90, 180]
    )
    arg_interest_rate = create_argument(
        name="interest_rate",
        description="The valuation interest rate.",
        dtype=float,
        min_val=0,
        max_val=0.1
    )

    # create list of steps
    steps = [
        {
            "name": "create-projected-frame",
            "function": create_projected_frame,
            "args": {"valuation_date": arg_valuation_date, "periods": 10}
        },
        {
            "name": "add-expected-benefit",
            "function": add_expected_benefit,
            "args": {"frame": use("create-projected-frame")}
        },
        {
            "name": "add-ctr",
            "function": add_ctr,
            "args": {
                "frame": use("add-expected-benefit"),
                "disabled_age": arg_disabled_age,
                "gender": arg_gender,
                "occ_class": arg_occ_class,
                "elim_period": arg_elim_period,
            }
        },
        {
            "name": "calculate-lives-inforce",
            "function": calculate_lives_inforce,
            "args": {"frame": use("add-ctr")}
        },
        {
            "name": "add-discount-factor",
            "function": add_discount_factor,
            "args": {
                "frame": use("calculate-lives-inforce"),
                "interest_rate": arg_interest_rate
            }
        },
        {
            "name": "calculate-pv-benefits",
            "function": calculate_pv_benefits,
            "args": {"frame": use("add-discount-factor")}
        },
    ]

Looking at the above code you will notice all arguments are assigned with a *use("...")* statement,
an argument, or the value is set (see create_projected_frame periods). When we use a *use("...")*
statement we are drawing a dependency from the output of one step to be the input to another step.
The above example is linear in the sense that each step's output is the input to the next step.
However, this sequence does not need to be linear where each step goes into the next step as input.
What happens with this logic will become more apparent after building the model and looking at the
docstring.

**Creating the Model**

To create the model run -

.. code-block:: python

    from footings import create_model

    DLRModel = create_model(
        name = "DLRModel",
        description = "This model calculates a disabled life reserve (DLR).",
        steps = steps
    )

This returns an object called DLRModel and it has the following attributes -

- valuation_date
- disabled_age
- gender
- occ_class
- elim_period
- interest_rate

These are all the function arguments that were passed assigned an argument. This can be viewed
by running -

.. code-block:: python

    help(DLRModel)
    # |  DLRModel(*, valuation_date, disabled_age, gender, occ_class, elim_period, interest_rate)
    # |
    # |  This model calculates a disabled life reserve (DLR).
    # |
    # |  Attributes
    # |  ----------
    # |  valuation_date
    # |          The valuation date.
    # |  disabled_age
    # |          The age the policy holder became disabled.
    # |  gender
    # |          The gender of the policy holder.
    # |  occ_class
    # |          The occupation class of the policy holder.
    # |  elim_period
    # |          The elimination period for the policy.
    # |  interest_rate
    # |          The valuation interest rate.
    # |
    # |  Returns
    # |  -------

In addition, note the starting * in the signature. All arguments passed to DLRModel must be keyword
arguments. Positional arguments are not accepted. When running models, it is better to be explicit
vs implicit especially as some models might have many arguments to pass.

All models created by the factory function, create_model, return a new class object that is a subclass
of the BaseModel. The BaseModel has a few important methods including -

- run - to run the model (i.e., trigger the computations)
- audit - a method that creates an excel file that can be used to audit the calculations of the model
- visualize - (not implemented)

**Instantiate the Model**

The factory function create_model only creates the class DLRModel. To run the DLR model, we must
first instantiate the model. To do this, pass the necessary arguments to the object.

.. code-block:: python

    dlr_init = DLRModel(
        valuation_date=pd.Timestamp("2019-12-31"),
        disabled_age=35,
        gender="M",
        occ_class=1,
        elim_period=14,
        interest_rate=0.04
    )

**Run the Model**

To run the model, call the run method on the instantiated model.

.. code-block:: python

    dlr_init.run()
    #   DATE	    DURATION    BENEFIT	CTR	LIVES       DISCOUNT    PV_BENEFITS
    # 0	2019-12-31	1       100     0.01	0.990000    0.961538	770.283113
    # 1	2020-01-31	2       100     0.01	0.980100    0.924556	675.090805
    # 2	2020-02-29	3       100     0.01	0.970299    0.888996	584.475051
    # 3	2020-03-31	4       100     0.01	0.960596    0.854804	498.215823
    # 4	2020-04-30	5       100     0.01	0.950990    0.821927	416.103673
    # 5	2020-05-31	6       100     0.01	0.941480    0.790315	337.939223
    # 6	2020-06-30	7       100     0.01	0.932065    0.759918	263.532680
    # 7	2020-07-31	8       100     0.01	0.922745    0.730690	192.703374
    # 8	2020-08-31	9       100     0.01	0.913517    0.702587	125.279323
    # 9	2020-09-30	10      100     0.01	0.904382    0.675564	61.096812


**Audit the Model**

To audit the model, call the audit method on the instantiated model.

.. code-block:: python

    dlr_init.audit("dlr-audit.xlsx")

Ending Note
-----------

This user guide walked through a simple example. To get a better understanding read about the
special objects and also read through other examples models which have been built with the
footings framework.
