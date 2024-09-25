# Atomic Datasets

A key concept for CFstore is that of an _atomic dataset_.  The notion is important, because each variable in CFstore is treated as an atomic_dataset, which means that if you have lots of files,  you really must aggregate your data using CFA _before_ uploading, otherwise each variable in each file will be treated as an atomic dataset, and you'll get flooded with information.

Once you have atomic datasets, we need them to be unique, so we can handle quarks (subsets, and/or replicants, in different locations) properly.

The Variable model class is the key one for instantiating all the important logic, but it is the Variable interface
that is used in the middleware logic.

::: core.models.Variable

::: core.db.interface.VariableInterface