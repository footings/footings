# Principles

The principles upon which the `footings` library was built are listed below with commentary on why they are a principle. Do note the `footings` library was designed with the intention of making it easier to develop long duration life and health actuarial models. Thus, the principles are heavily inspired by these types of models.

## Models are a sequence of linked steps

Actuarial models usually require many calculations and data processing steps to transform inputs into the needed output. Examples of calculations and data processing steps typical in actuarial models include -

- calculating dates or ages of policy holders,
- loading assumption values from disk (e.g., csv file),
- adding, subtracting, multiplying, dividing vectors,
- summing values of groups, and
- data wrangling to get needed format.

The `footings framework` needs to embrace this thought and make it easy to do siloed calculations and data processing steps that are linked together to form the final output.

## Models need to be easy to understand

For programmers, it is often said that code is read more often then it is written. Actuarial models have a similar situation after initial development, where models will more often be reviewed by those who did not do the initial development then modified in any meaningful way. Thus, the `footings framework` needs to embrace this. Objects used to build models need to have meaningful names and they need to come together in a way that makes sense. The closer we get it to pseudocode the better. Simply using python as the underlying language helps considerably given its readability.

## Models need to have validation built in

Actuarial models tend to have lots of inputs covering all data types - strings, ints, floats, dates, etc. Some of these have specifications such as a min value or max value, only a list of values, etc. Being able to specify data types and rules around those data types will aid in finding errors early that may impact downstream steps within a model.

## Models need to be easy to audit

Actuarial models are used to produce values that flow through to financial statements, price insurance, and inform regulators of solvency. It is vital that those using the models understand what is going on and be able to explain how the ending numbers were derived. Often times, actuaries that use PAMS are forced to maintain separate emulators in MS Excel that replicate the results of the model. These emulators are used for serving internal and external audit needs as well as informing those who are not familiar with the system how the  model works. Creating these emulator can be very time consuming given some PAMS are closed systems where you cannot see the underlying code. Given the importance of the emulators in a different environment, the `footings framework` makes it easy to produce an audit file that shows the steps on how the numbers were derived.

## Models need to be self documenting

All to often documentation supporting actuarial models exist as a MS word document, which is a less than ideal tool and delivery format for the task. There are a couples reasons for this including that documentation is not usually treated as important as code. It is often completed last when building a model and the effort towards it is skimped. In addition, documentation typically is not versioned like code is in git/github workflow.

The `footings framework` wants to make it easy for model developers to provide professional, end user level documentation. This can be accomplished by building off the Sphinx documentation pipeline and customizing how model objects are displayed  in Sphinx. This pushes documentation down to the model developer and versions the documentation as well.

## Models need to be able to scale up

Actuarial models for long duration products are compute intensive as calculations need to be performed like a matrix across many policy holders and scenarios. In order for the `footings framework` to be a realistic solution for actuarial modeling, it will need to scale-up to use additional compute resources. Good news there exist well supported python frameworks to do high performance computing such as [dask](https://dask.org/) and [ray](https://ray.io/) which can be utilized with the `footings framework`.

## Models need to be able to build off other models

Insurance products can have many features and the number of features can grow considerably when there are many insurance riders. Many of these features require slight variations in calculations or extra inputs when modeling. In order to model all these features in a concise way, models needs to be able to be combined. Given the `footings framework` is written in python, it allows inheritance just like standard python classes. In addition, the `footings framework` provides key objects that allow policy models (i.e., models for a single policy) to be combined to form population models (i.e., models for many policies).

## Model environments should not be monolithic

Long duration actuarial models usually are built with PAMS. PAMS are monolithic systems. That is, they are designed to be the one tool solution with an exception for connectivity to databases for i/o. This is their biggest downfall. It is hard to combine PAMS with other software including all the DevOps tooling that has been created over the years. Features have to come through the vendor of the PAMS. Thus, innovation is slow and lags significantly behind what software developers have come to expect. The `footings framework` was designed with this mind and is the reason why the project is open-source and exist as pure python. This allows the most flexibility to combine a mix of software to create a best in class modeling environment.
