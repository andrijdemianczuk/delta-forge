# Delta Forge PySpark Helper
Delta Forge is a set of tools to help users work with and quickly format PySpark objects for use with Delta storage.

Although primarily used with Databricks, Delta Forge also supports [OSS Delta](https://delta.io) for use in your own environment.

## Installation

You can easily install Delta Forge from [PyPi](https://pypi.org):
```shell
pip install deltaforge
```
## How to use
The library is mostly used as an instantiated object. Once you have an instance, you can call any of the class's behaviors or attributes. New classed will be added on a regular basis.

```python
from deltaforge.format.DeltaDataframeHelper import DeltaDataframeHelper

# Instance the DeltaDatafameHelper object
dfh = DeltaDataframeHelper()

# Replace all instances of "," with "." in a dataframe called df
df = dfh.substringReplaceData(col_names=['col1', 'col2'])(df=df, findChars=",", replaceChars=".")

# Using the column fixer to set all cols to lowercase with stripped out whitespaces
fixed_cols = dfh.formatDataframeCols(df=df)
df = df.selectExpr(fixed_cols)

# Cast a group of columns to a specific data type (Double for this example)
from pyspark.sql.types import DoubleType
cols_to_cast = ['col1', 'col2']
df = dfh.castColTypes(cols_to_cast)(df=df, targetType=DoubleType())

```